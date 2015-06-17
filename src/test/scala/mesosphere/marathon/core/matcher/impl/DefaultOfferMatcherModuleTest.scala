package mesosphere.marathon.core.matcher.impl

import mesosphere.marathon.MarathonTestHelper
import mesosphere.marathon.core.base.actors.ActorsModule
import mesosphere.marathon.core.base.{ Clock, ShutdownHooks }
import mesosphere.marathon.core.leadership.AlwaysElectedLeadershipModule
import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherModule }
import mesosphere.marathon.core.task.bus.TaskBusModule
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.{ CommandInfo, Offer, Resource, SlaveID, TaskID, TaskInfo, Value }
import org.scalatest.{ BeforeAndAfter, FunSuite }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.Random

class DefaultOfferMatcherModuleTest extends FunSuite with BeforeAndAfter {

  // FIXME: Missing Tests
  // Adding matcher while matching offers
  // Removing matcher while matching offers, removed matcher does not get offers anymore
  // Timeout for matching
  // Respect launch tokens for launching
  // resource calculation throws exception, matching continues
  // Deal with randomness?

  test("no registered matchers result in empty result") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer().build()
    val matchedTasksFuture: Future[MatchedTasks] =
      module.globalOfferMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks.isEmpty)
  }

  test("single offer is passed to matcher") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer(cpus = 1.0).build()

    val task = makeOneCPUTask("task1")
    module.subOfferMatcherManager.addOfferMatcher(new ConstantCPUOfferMatcher(Seq(task)))

    val matchedTasksFuture: Future[MatchedTasks] =
      module.globalOfferMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks == Seq(task))
  }

  test("deregistering only matcher works") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer(cpus = 1.0).build()

    val task = makeOneCPUTask("task1")
    val matcher: ConstantCPUOfferMatcher = new ConstantCPUOfferMatcher(Seq(task))
    module.subOfferMatcherManager.addOfferMatcher(matcher)
    module.subOfferMatcherManager.removeOfferMatcher(matcher)

    val matchedTasksFuture: Future[MatchedTasks] =
      module.globalOfferMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks.isEmpty)
  }

  test("single offer is passed to multiple matchers") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer(cpus = 2.0).build()

    val task1: TaskInfo = makeOneCPUTask("task1")
    module.subOfferMatcherManager.addOfferMatcher(new ConstantCPUOfferMatcher(Seq(task1)))
    val task2: TaskInfo = makeOneCPUTask("task2")
    module.subOfferMatcherManager.addOfferMatcher(new ConstantCPUOfferMatcher(Seq(task2)))

    val matchedTasksFuture: Future[MatchedTasks] =
      module.globalOfferMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks.toSet == Set(task1, task2))
  }

  test("single offer is passed to multiple matchers repeatedly") {
    val offer: Offer = MarathonTestHelper.makeBasicOffer(cpus = 4.0).build()

    val task1: TaskInfo = makeOneCPUTask("task1")
    module.subOfferMatcherManager.addOfferMatcher(new ConstantCPUOfferMatcher(Seq(task1)))
    val task2: TaskInfo = makeOneCPUTask("task2")
    module.subOfferMatcherManager.addOfferMatcher(new ConstantCPUOfferMatcher(Seq(task2)))

    val matchedTasksFuture: Future[MatchedTasks] =
      module.globalOfferMatcher.processOffer(clock.now() + 1.second, offer)
    val matchedTasks: MatchedTasks = Await.result(matchedTasksFuture, 3.seconds)
    assert(matchedTasks.tasks.toSet == Set(task1, task2, task1, task2))
  }

  def makeOneCPUTask(idBase: String) = {
    MarathonTestHelper.makeOneCPUTask(idBase).build()
  }

  private[this] var module: OfferMatcherModule = _
  private[this] var shutdownHookModule: ShutdownHooks = _
  private[this] var clock: Clock = _

  before {
    shutdownHookModule = ShutdownHooks()
    clock = Clock()
    val random = Random
    val actorSystem = AlwaysElectedLeadershipModule(shutdownHookModule)
    val taskBusModule = TaskBusModule()
    module = OfferMatcherModule(
      clock, random, actorSystem, taskBusModule.taskStatusEmitter, taskBusModule.taskStatusObservables)
  }

  after {
    shutdownHookModule.shutdown()
  }

  /**
    * Simplistic matcher which only looks if there are sufficient CPUs in the offer
    * for the given tasks. It has no state and thus continues matching infinitely.
    */
  private class ConstantCPUOfferMatcher(tasks: Seq[TaskInfo]) extends OfferMatcher {
    import scala.collection.JavaConverters._

    var results = Vector.empty[MatchedTasks]

    val totalCpus: Double = {
      val cpuValues = for {
        task <- tasks
        resource <- task.getResourcesList.asScala
        if resource.getName == "cpus"
        cpuScalar <- Option(resource.getScalar)
        cpus = cpuScalar.getValue
      } yield cpus
      cpuValues.sum
    }

    override def processOffer(deadline: Timestamp, offer: Offer): Future[MatchedTasks] = {
      val cpusInOffer: Double =
        offer.getResourcesList.asScala.find(_.getName == "cpus")
          .flatMap(r => Option(r.getScalar))
          .map(_.getValue)
          .getOrElse(0)

      val resultTasks: Seq[TaskInfo] = if (cpusInOffer >= totalCpus) tasks else Seq.empty
      val result = MatchedTasks(offer.getId, resultTasks)
      results :+= result
      Future.successful(result)
    }
  }
}
