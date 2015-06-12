package mesosphere.marathon.state

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.tasks.TaskTracker
import mesosphere.marathon.upgrade.DeploymentPlan
import mesosphere.marathon.{ MarathonSchedulerService, MarathonSpec }
import org.scalatest.mock.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers }
import org.mockito.Mockito._
import org.mockito.Matchers.{ any, anyBoolean }

import scala.concurrent.Future

class TaskKillerTest extends MarathonSpec
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with ScalaFutures {

  var tracker: TaskTracker = _
  var service: MarathonSchedulerService = _
  var groupManager: GroupManager = _
  var taskKiller: TaskKiller = _

  before {
    service = mock[MarathonSchedulerService]
    tracker = mock[TaskTracker]
    groupManager = mock[GroupManager]
    taskKiller = new TaskKiller(tracker, groupManager, service)
  }

  test("AppNotFound") {
    val appId = PathId("invalid")
    when(tracker.contains(appId)).thenReturn(false)

    val result = taskKiller.kill(appId, (tasks) => Set.empty[MarathonTask], scale = true)
    result.futureValue shouldEqual TaskKiller.AppNotFound(appId)
  }

  test("GroupNotFound") {
    val appId = PathId(List("my", "app"))
    when(tracker.contains(appId)).thenReturn(true)
    when(groupManager.group(appId.parent)).thenReturn(Future.successful(None))

    val result = taskKiller.kill(appId, (tasks) => Set.empty[MarathonTask], scale = true)
    result.futureValue shouldEqual TaskKiller.GroupNotFound(appId.parent)
  }

  test("KillRequested") {
    val appId = PathId(List("my", "app"))
    val tasksToKill = Set(MarathonTask.getDefaultInstance)
    when(tracker.contains(appId)).thenReturn(true)
    when(groupManager.group(appId.parent)).thenReturn(Future.successful(Some(Group.emptyWithId(appId.parent))))
    when(groupManager.updateApp(any[PathId], any(), any[Timestamp](), anyBoolean(), any[Set[MarathonTask]]())).thenReturn(
      Future.successful(DeploymentPlan.empty)
    )

    val result = taskKiller.kill(appId, (tasks) => tasksToKill, scale = true)
    result.futureValue shouldEqual TaskKiller.KillRequested(tasksToKill)
  }

  test("KillRequested if scale is false") {
    val appId = PathId(List("my", "app"))
    val tasksToKill = Set(MarathonTask.getDefaultInstance)
    when(tracker.contains(appId)).thenReturn(true)

    val result = taskKiller.kill(appId, (tasks) => tasksToKill, scale = false)
    result.futureValue shouldEqual TaskKiller.KillRequested(tasksToKill)
    verify(service, times(1)).killTasks(appId, tasksToKill)
    verifyNoMoreInteractions(groupManager)
  }

}
