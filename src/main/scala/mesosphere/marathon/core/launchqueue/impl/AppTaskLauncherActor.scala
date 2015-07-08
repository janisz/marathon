package mesosphere.marathon.core.launchqueue.impl

import akka.actor.{ Cancellable, Actor, ActorLogging, ActorRef, Props }
import akka.event.LoggingReceive
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedTaskCount
import mesosphere.marathon.core.launchqueue.impl.AppTaskLauncherActor.RecheckDoNoLaunchBefore
import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherManager }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusObservables }
import mesosphere.marathon.state.{ AppDefinition, Timestamp }
import mesosphere.marathon.tasks.TaskFactory.CreatedTask
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }
import org.apache.mesos.Protos.TaskID
import rx.lang.scala.Subscription

import scala.concurrent.duration._

private[impl] object AppTaskLauncherActor {
  def props(
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskFactory: TaskFactory,
    taskStatusObservable: TaskStatusObservables,
    taskTracker: TaskTracker,
    rateLimiterActor: ActorRef)(
      app: AppDefinition,
      initialCount: Int): Props = {
    Props(new AppTaskLauncherActor(
      offerMatcherManager,
      clock, taskFactory, taskStatusObservable, taskTracker, rateLimiterActor,
      app, initialCount))
  }

  sealed trait Requests

  /**
    * Increase the task count of the receiver.
    * The actor responds with a [[QueuedTaskCount]] message.
    */
  case class AddTasks(app: AppDefinition, count: Int) extends Requests
  /**
    * Get the current count.
    * The actor responds with a [[QueuedTaskCount]] message.
    */
  case object GetCount extends Requests

  /**
   * Results in rechecking whether we may launch tasks.
   */
  private case object RecheckDoNoLaunchBefore extends Requests
}

/**
  * Allows processing offers for starting tasks for the given app.
  */
private class AppTaskLauncherActor(
  offerMatcherManager: OfferMatcherManager,
  clock: Clock,
  taskFactory: TaskFactory,
  taskStatusObservable: TaskStatusObservables,
  taskTracker: TaskTracker,
  rateLimiterActor: ActorRef,
  initialApp: AppDefinition,
  initialTasksToLaunch: Int)
    extends Actor with ActorLogging {

  private[this] var app = initialApp

  private[this] var tasksToLaunch = initialTasksToLaunch
  private[this] var inFlightTaskLaunches = Set.empty[TaskID]
  private[this] var doNotLaunchBefore: Option[Timestamp] = None

  /** Manage registering this actor as offer matcher. Only register it if tasksToLaunch > 0. */
  private[this] object OfferMatcherRegistration {
    private[this] val myselfAsOfferMatcher: OfferMatcher = new ActorOfferMatcher(clock, self)
    private[this] var registeredAsMatcher = false

    /** Register/unregister as necessary */
    def manageOfferMatcherStatus(): Unit = {
      val shouldBeRegistered = shouldLaunchTasks

      if (shouldBeRegistered && !registeredAsMatcher) {
        log.debug("Start receiving offers for {}, {}", app.id, app.version)
        offerMatcherManager.addOfferMatcher(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = true
      }
      else if (!shouldBeRegistered && registeredAsMatcher) {
        log.debug("Stop receiving offers for {}, {}", app.id, app.version)
        offerMatcherManager.removeOfferMatcher(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }

    def unregister(): Unit = {
      if (registeredAsMatcher) {
        offerMatcherManager.removeOfferMatcher(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }
  }

  private[this] var taskStatusUpdateSubscription: Subscription = _

  private[this] var runningTasks: Set[MarathonTask] = _
  private[this] var runningTasksMap: Map[String, MarathonTask] = _

  private[this] var getInitialDelay: Option[Cancellable] = _

  override def preStart(): Unit = {
    super.preStart()

    log.info("Started appTaskLaunchActor for {} version {} with initial count {}",
      app.id, app.version, initialTasksToLaunch)

    taskStatusUpdateSubscription = taskStatusObservable.forAppId(app.id).subscribe(self ! _)
    runningTasks = taskTracker.get(app.id)
    runningTasksMap = runningTasks.map(task => task.getId -> task).toMap

    import context.dispatcher
    getInitialDelay = Some(context.system.scheduler.schedule(
      0.seconds, 100.millis,
      rateLimiterActor, RateLimiterActor.GetDelay(app)))
  }

  override def postStop(): Unit = {
    taskStatusUpdateSubscription.unsubscribe()
    getInitialDelay.foreach(_.cancel())

    super.postStop()

    log.info("Stopped appTaskLaunchActor for {} version {}", app.id, app.version)
  }

  override def receive: Receive = LoggingReceive {
    Seq(
      receiveDelayUpdate,
      receiveTaskStatusUpdate,
      receiveGetCurrentCount,
      receiveAddCount,
      receiveProcessOffers
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveDelayUpdate: Receive = {
    case RateLimiterActor.DelayUpdate(delayApp, delayUntil) if delayApp == app =>
      getInitialDelay.foreach(_.cancel())
      getInitialDelay = None
      doNotLaunchBefore = Some(delayUntil)
      OfferMatcherRegistration.manageOfferMatcherStatus()

      val now: Timestamp = clock.now()
      if (delayUntil > now) {
        import context.dispatcher
        context.system.scheduler.scheduleOnce(now until delayUntil, self, RecheckDoNoLaunchBefore)
      }

    case RecheckDoNoLaunchBefore => OfferMatcherRegistration.manageOfferMatcherStatus()
  }

  private[this] def receiveTaskStatusUpdate: Receive = {
    case TaskStatusUpdate(_, taskId, MarathonTaskStatus.LaunchDenied) if inFlightTaskLaunches(taskId) =>
      removeTask(taskId)
      tasksToLaunch += 1
      log.debug("Task launch for {} was denied, rescheduling, {} task launches remain unconfirmed",
        taskId, inFlightTaskLaunches.size)
      OfferMatcherRegistration.manageOfferMatcherStatus()

    case TaskStatusUpdate(_, taskId, MarathonTaskStatus.LaunchRequested) =>
      inFlightTaskLaunches -= taskId
      log.debug("Task launch for {} was requested, {} task launches remain unconfirmed",
        taskId, inFlightTaskLaunches.size)

    case TaskStatusUpdate(_, taskId, MarathonTaskStatus.Terminal(_)) =>
      removeTask(taskId)
  }

  private[this] def removeTask(taskId: TaskID): Unit = {
    inFlightTaskLaunches -= taskId
    runningTasksMap.get(taskId.getValue).foreach { marathonTask =>
      runningTasksMap -= taskId.getValue
      runningTasks -= marathonTask
    }
  }

  private[this] def receiveGetCurrentCount: Receive = {
    case AppTaskLauncherActor.GetCount =>
      replyWithQueuedTaskCount()
  }

  private[this] def receiveAddCount: Receive = {
    case AppTaskLauncherActor.AddTasks(newApp, addCount) =>
      if (app != newApp) {
        app = newApp
        log.info("getting new app definition for {}, version {}", app.id, app.version)
      }

      tasksToLaunch += addCount
      OfferMatcherRegistration.manageOfferMatcherStatus()

      replyWithQueuedTaskCount()
  }

  private[this] def replyWithQueuedTaskCount(): Unit = {
    sender() ! QueuedTaskCount(
      app,
      tasksLeftToLaunch = tasksToLaunch,
      taskLaunchesInFlight = inFlightTaskLaunches.size,
      tasksLaunchedOrRunning = runningTasks.size - inFlightTaskLaunches.size
    )
  }

  private[this] def receiveProcessOffers: Receive = {
    case ActorOfferMatcher.MatchOffer(deadline, offer) if clock.now() > deadline || !shouldLaunchTasks =>
      sender ! MatchedTasks(offer.getId, Seq.empty)

    case ActorOfferMatcher.MatchOffer(deadline, offer) =>
      val newTaskOpt: Option[CreatedTask] = taskFactory.newTask(app, offer, runningTasks)
      val tasks = newTaskOpt match {
        case Some(CreatedTask(mesosTask, marathonTask)) =>
          // move to short before launch, batch
          taskTracker.created(app.id, marathonTask)

          runningTasks += marathonTask
          runningTasksMap += marathonTask.getId -> marathonTask
          inFlightTaskLaunches += mesosTask.getTaskId

          tasksToLaunch -= 1
          OfferMatcherRegistration.manageOfferMatcherStatus()

          import context.dispatcher
          // timeout
          context.system.scheduler.scheduleOnce(
            3.seconds,
            self,
            TaskStatusUpdate(clock.now(), mesosTask.getTaskId, MarathonTaskStatus.LaunchDenied))

          Seq(mesosTask)
        case None => Seq.empty
      }
      sender ! MatchedTasks(offer.getId, tasks)
  }

  private[this] def shouldLaunchTasks: Boolean =
    tasksToLaunch > 0 && doNotLaunchBefore.exists(_ < clock.now())
}
