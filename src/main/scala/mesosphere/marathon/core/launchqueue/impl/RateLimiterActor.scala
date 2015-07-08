package mesosphere.marathon.core.launchqueue.impl

import akka.actor.{ Props, Actor, ActorLogging, ActorRef }
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.launchqueue.impl.RateLimiterActor.{ AddDelay, DecreaseDelay, DelayUpdate, GetDelay, ResetDelay, ResetDelayResponse }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusObservables }
import mesosphere.marathon.state.{ AppDefinition, AppRepository, Timestamp }
import mesosphere.marathon.tasks.{ TaskIdUtil, TaskTracker }
import org.apache.mesos.Protos.TaskID
import rx.lang.scala.Subscription

import scala.concurrent.Future

private[impl] object RateLimiterActor {
  def props(
    rateLimiter: RateLimiter,
    taskTracker: TaskTracker,
    appRepository: AppRepository,
    updateReceiver: ActorRef,
    taskStatusObservables: TaskStatusObservables) = Props(new RateLimiterActor(rateLimiter, taskTracker, appRepository, updateReceiver, taskStatusObservables))

  case class DelayUpdate(app: AppDefinition, delayUntil: Timestamp)

  case class ResetDelay(app: AppDefinition)
  case object ResetDelayResponse

  case class GetDelay(appDefinition: AppDefinition)
  private case class AddDelay(app: AppDefinition)
  private case class DecreaseDelay(app: AppDefinition)
}

private[impl] class RateLimiterActor private (
    rateLimiter: RateLimiter,
    taskTracker: TaskTracker,
    appRepository: AppRepository,
    updateReceiver: ActorRef,
    taskStatusObservables: TaskStatusObservables) extends Actor with ActorLogging {
  var taskStatusSubscription: Subscription = _

  override def preStart(): Unit = {
    taskStatusSubscription = taskStatusObservables.forAll.subscribe(self ! _)
  }

  override def postStop(): Unit = {
    taskStatusSubscription.unsubscribe()
  }

  override def receive: Receive = {
    case GetDelay(app) =>
      sender() ! DelayUpdate(app, rateLimiter.getDelay(app))

    case AddDelay(app) =>
      rateLimiter.addDelay(app)
      updateReceiver ! DelayUpdate(app, rateLimiter.getDelay(app))

    case DecreaseDelay(app) =>
      rateLimiter.decreaseDelay(app)
      updateReceiver ! DelayUpdate(app, rateLimiter.getDelay(app))

    case ResetDelay(app) =>
      rateLimiter.resetDelay(app)
      sender() ! ResetDelayResponse

    case TaskStatusUpdate(_, taskId, status) =>
      status match {
        case MarathonTaskStatus.Error(_) | MarathonTaskStatus.Lost(_) | MarathonTaskStatus.Failed(_) =>
          sendToSelfForApp(taskId, AddDelay(_))

        case MarathonTaskStatus.Running(_) =>
          sendToSelfForApp(taskId, DecreaseDelay(_))

        case _ => // Ignore
      }
  }

  private[this] def sendToSelfForApp(taskId: TaskID, toMessage: AppDefinition => Any): Unit = {
    val appId = TaskIdUtil.appId(taskId)
    val maybeTask: Option[MarathonTask] = taskTracker.fetchTask(appId, taskId.getValue)
    val maybeAppFuture: Future[Option[AppDefinition]] = maybeTask.map { task =>
      appRepository.app(appId, Timestamp(task.getVersion))
    }.getOrElse(Future.successful(None))

    import context.dispatcher
    maybeAppFuture.foreach(self ! _.map(toMessage))
  }
}
