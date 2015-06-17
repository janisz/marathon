package mesosphere.marathon.core.task.bus.impl

import mesosphere.marathon.core.task.bus.{ TaskStatusUpdateTestHelper, TaskBusModule }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.state.PathId
import org.scalatest.{ BeforeAndAfter, FunSuite }

class DefaultTaskStatusModuleTest extends FunSuite with BeforeAndAfter {
  var module: TaskBusModule = _
  before {
    module = new DefaultTaskBusModule
  }

  test("observable forAll includes all app status updates") {
    var received = List.empty[TaskStatusUpdate]
    module.taskStatusObservables.forAll.foreach(received :+= _)
    val aa: TaskStatusUpdate = TaskStatusUpdateTestHelper.running.withAppId("/a/a").wrapped
    val ab: TaskStatusUpdate = TaskStatusUpdateTestHelper.running.withAppId("/a/b").wrapped
    module.taskStatusEmitter.publish(aa)
    module.taskStatusEmitter.publish(ab)
    assert(received == List(aa, ab))
  }

  test("observable forAppId includes only app status updates") {
    var received = List.empty[TaskStatusUpdate]
    module.taskStatusObservables.forAppId(PathId("/a/a")).foreach(received :+= _)
    val aa: TaskStatusUpdate = TaskStatusUpdateTestHelper.running.withAppId("/a/a").wrapped
    val ab: TaskStatusUpdate = TaskStatusUpdateTestHelper.running.withAppId("/a/b").wrapped
    module.taskStatusEmitter.publish(aa)
    module.taskStatusEmitter.publish(ab)
    assert(received == List(aa))
  }
}
