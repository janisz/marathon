package mesosphere.marathon.core.task.bus

object MarathonTaskStatusTestHelper {
  val running = MarathonTaskStatus.Running(mesosStatus = None)
  val staging = MarathonTaskStatus.Staging(mesosStatus = None)
}
