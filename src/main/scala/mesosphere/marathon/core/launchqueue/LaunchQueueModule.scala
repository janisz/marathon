package mesosphere.marathon.core.launchqueue

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launchqueue.impl.DefaultLaunchQueueModule
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.OfferMatcherManager
import mesosphere.marathon.core.task.bus.TaskStatusObservables
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }

private[core] trait LaunchQueueModule {
  def taskQueue: LaunchQueue
}

object LaunchQueueModule {
  def apply(
    leadershipModule: LeadershipModule,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    taskStatusObservables: TaskStatusObservables,
    taskTracker: TaskTracker,
    taskFactory: TaskFactory): LaunchQueueModule = new DefaultLaunchQueueModule(
    leadershipModule,
    clock,
    subOfferMatcherManager,
    taskStatusObservables,
    taskTracker,
    taskFactory
  )
}
