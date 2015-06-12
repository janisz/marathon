package mesosphere.marathon.upgrade

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.tasks.MarathonTasks
import org.scalatest.{ FunSuite, Matchers }

class ScalingPropositionTest extends FunSuite with Matchers {

  test("propose - empty tasksToKill should lead to ScalingProposition(None, _)") {
    val running = Set.empty[MarathonTask]
    val toKill = Some(Set.empty[MarathonTask])
    val scaleTo = 0
    val proposition = ScalingProposition.propose(running, toKill, scaleTo)
    proposition.tasksToKill shouldBe empty
  }

  test("propose - nonEmpty tasksToKill should be ScalingProposition(Some(_), _)") {
    val task = MarathonTask.getDefaultInstance
    val running = Set(task)
    val toKill = Some(Set(task))
    val scaleTo = 0
    val proposition = ScalingProposition.propose(running, toKill, scaleTo)
    proposition.tasksToKill shouldEqual Some(Seq(task))
  }

  test("propose - scaleTo = 0 should be ScalingProposition(_, None)") {
    val running = Set.empty[MarathonTask]
    val toKill = Some(Set.empty[MarathonTask])
    val scaleTo = 0
    val proposition = ScalingProposition.propose(running, toKill, scaleTo)
    proposition.tasksToStart shouldBe empty
  }

  test("propose - negative scaleTo should be ScalingProposition(_, None)") {
    val running = Set.empty[MarathonTask]
    val toKill = Some(Set.empty[MarathonTask])
    val scaleTo = -42
    val proposition = ScalingProposition.propose(running, toKill, scaleTo)
    proposition.tasksToStart shouldBe empty
  }

  test("propose - positive scaleTo should be ScalingProposition(_, Some(_))") {
    val running = Set.empty[MarathonTask]
    val toKill = Some(Set.empty[MarathonTask])
    val scaleTo = 42

    val proposition = ScalingProposition.propose(running, toKill, scaleTo)
    proposition.tasksToStart shouldBe Some(42)
  }

  test("Determine tasks to kill when there are no sentenced") {
    val runningTasks = Set.empty[MarathonTask]
    val sentencedToDeath = Some(Set.empty[MarathonTask])
    val scaleTo = 5
    val proposition = ScalingProposition.propose(runningTasks, sentencedToDeath, scaleTo)
    proposition.tasksToKill shouldBe empty
    proposition.tasksToStart shouldBe Some(5)
  }

  test("Determine tasks to kill and start when none are sentenced and need to scale") {
    val runningTasks: Set[MarathonTask] = Set(
      createTask(1),
      createTask(2),
      createTask(3)
    )
    val sentencedToDeath = Some(Set.empty[MarathonTask])
    val scaleTo = 5
    val proposition = ScalingProposition.propose(runningTasks, sentencedToDeath, scaleTo)
    proposition.tasksToKill shouldBe empty
    proposition.tasksToStart shouldBe Some(2)
  }

  test("Determine tasks to kill when scaling to 0") {
    val runningTasks: Set[MarathonTask] = Set(
      createTask(1),
      createTask(2),
      createTask(3)
    )
    val sentencedToDeath = Some(Set.empty[MarathonTask])
    val scaleTo = 0
    val proposition = ScalingProposition.propose(runningTasks, sentencedToDeath, scaleTo)
    proposition.tasksToKill shouldBe defined
    proposition.tasksToKill.get shouldEqual runningTasks.toSeq.reverse
    proposition.tasksToStart shouldBe empty
  }

  test("Determine tasks to kill w/ invalid task") {
    val task_1 = createTask(1)
    val task_2 = createTask(2)
    val task_3 = createTask(3)

    val runningTasks: Set[MarathonTask] = Set(task_1, task_2, task_3)
    val sentencedToDeath = Some(Set(
      task_2,
      task_3,
      MarathonTasks.makeTask("alreadyKilled", "", Nil, Nil, version = Timestamp(0L))))
    val scaleTo = 3
    val proposition = ScalingProposition.propose(runningTasks, sentencedToDeath, scaleTo)

    proposition.tasksToKill shouldBe defined
    proposition.tasksToKill.get shouldEqual Seq(task_2, task_3)
    proposition.tasksToStart shouldBe Some(2)
  }

  test("Determine tasks to kill w/ invalid task 2") {
    val task_1 = createTask(1)
    val task_2 = createTask(2)
    val task_3 = createTask(3)
    val task_4 = createTask(4)

    val runningTasks: Set[MarathonTask] = Set(task_1, task_2, task_3, task_4)
    val sentencedToDeath = Some(Set(createTask(42)))
    val scaleTo = 3
    val proposition = ScalingProposition.propose(runningTasks, sentencedToDeath, scaleTo)

    proposition.tasksToKill shouldBe defined
    proposition.tasksToKill.get shouldEqual Seq(task_4)
    proposition.tasksToStart shouldBe empty
  }

  private def createTask(index: Long) = MarathonTasks.makeTask(s"task-$index", "", Nil, Nil, version = Timestamp(index))

}
