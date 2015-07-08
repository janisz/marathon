package mesosphere.marathon.core.launchqueue.impl

import akka.actor.{ ActorRef, Props }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launchqueue.{ LaunchQueue, LaunchQueueModule }
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.OfferMatcherManager
import mesosphere.marathon.core.task.bus.TaskStatusObservables
import mesosphere.marathon.state.{ AppRepository, AppDefinition }
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }

private[core] class DefaultLaunchQueueModule(
    leadershipModule: LeadershipModule,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    taskStatusObservables: TaskStatusObservables,
    appRepository: AppRepository,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory) extends LaunchQueueModule {

  override lazy val taskQueue: LaunchQueue = new ActorLaunchQueue(taskQueueActorRef)

  private[this] lazy val rateLimiter: RateLimiter = new RateLimiter(clock)

  private[this] lazy val rateLimiterActor: ActorRef = {
    val props = RateLimiterActor.props(
      rateLimiter, taskTracker, appRepository, taskQueueActorRef, taskStatusObservables)
    leadershipModule.startWhenLeader(props, "rateLimiter")
  }

  private[this] def appActorProps(app: AppDefinition, count: Int): Props =
    AppTaskLauncherActor.props(
      subOfferMatcherManager,
      clock,
      taskFactory,
      taskStatusObservables,
      taskTracker,
      rateLimiterActor)(app, count)

  private[impl] lazy val taskQueueActorRef: ActorRef = {
    val props = LaunchQueueActor.props(appActorProps)
    leadershipModule.startWhenLeader(props, "launchQueue")
  }

}
