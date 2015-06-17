package mesosphere.marathon.core.task.bus

import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.tasks.TaskIdUtil
import org.joda.time.DateTime

class TaskStatusUpdateTestHelper(val wrapped: TaskStatusUpdate) {
  def withAppId(appId: String): TaskStatusUpdateTestHelper = TaskStatusUpdateTestHelper {
    wrapped.copy(taskId = TaskStatusUpdateTestHelper.newTaskID(appId))
  }
}

object TaskStatusUpdateTestHelper {
  def apply(update: TaskStatusUpdate): TaskStatusUpdateTestHelper =
    new TaskStatusUpdateTestHelper(update)

  private def newTaskID(appId: String) = {
    TaskIdUtil.newTaskId(PathId(appId))
  }

  val taskId = newTaskID("/app")

  val running = TaskStatusUpdateTestHelper(
    TaskStatusUpdate(
      timestamp = Timestamp.apply(new DateTime(2015, 2, 3, 12, 30, 0, 0)),
      taskId = taskId,
      status = MarathonTaskStatusTestHelper.running
    )
  )

  val staging = TaskStatusUpdateTestHelper(
    TaskStatusUpdate(
      timestamp = Timestamp.apply(new DateTime(2015, 2, 3, 12, 31, 0, 0)),
      taskId = taskId,
      status = MarathonTaskStatusTestHelper.running
    )
  )
}
