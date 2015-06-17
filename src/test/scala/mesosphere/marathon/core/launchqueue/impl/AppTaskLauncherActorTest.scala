package mesosphere.marathon.core.launchqueue.impl

import mesosphere.marathon.core.base.{ Clock, ShutdownHooks }
import mesosphere.marathon.core.leadership.AlwaysElectedLeadershipModule
import mesosphere.marathon.core.matcher.DummyOfferMatcherManager
import mesosphere.marathon.core.task.bus.TaskBusModule
import mesosphere.marathon.state.PathId
import mesosphere.marathon.tasks.{ TaskFactory, TaskTracker }
import mesosphere.marathon.{ MarathonSpec, MarathonTestHelper }
import org.mockito.Mockito.verifyNoMoreInteractions
import org.scalatest.BeforeAndAfter

class AppTaskLauncherActorTest extends MarathonSpec with BeforeAndAfter {

  private[this] val app = MarathonTestHelper.makeBasicApp().copy(id = PathId("/app"))

  private[this] var shutdownHooks: ShutdownHooks = _
  private[this] var clock: Clock = _
  private[this] var taskBusModule: TaskBusModule = _
  private[this] var offerMatcherManager: DummyOfferMatcherManager = _
  private[this] var taskTracker: TaskTracker = _
  private[this] var taskFactory: TaskFactory = _
  private[this] var module: DefaultLaunchQueueModule = _

  private[this] def taskQueue = module.taskQueue
  private[this] def actorRef = module.taskQueueActorRef

  before {
    shutdownHooks = ShutdownHooks()
    val leadershipModule = AlwaysElectedLeadershipModule(shutdownHooks)
    clock = Clock()
    taskBusModule = TaskBusModule()

    offerMatcherManager = new DummyOfferMatcherManager()
    taskTracker = mock[TaskTracker]("taskTracker")
    taskFactory = mock[TaskFactory]("taskFactory")

    module = new DefaultLaunchQueueModule(
      leadershipModule,
      clock,
      subOfferMatcherManager = offerMatcherManager,
      taskStatusObservables = taskBusModule.taskStatusObservables,
      taskTracker,
      taskFactory
    )
  }

  after {
    verifyNoMoreInteractions(taskTracker)
    verifyNoMoreInteractions(taskFactory)

    shutdownHooks.shutdown()
  }
}
