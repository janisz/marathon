package mesosphere.marathon.core.matcher.impl

import akka.actor.ActorRef
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherManager, OfferMatcherModule }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusEmitter, TaskStatusObservables }

import scala.util.Random

private[matcher] class DefaultOfferMatcherModule(
  clock: Clock,
  random: Random,
  leadershipModule: LeadershipModule,
  taskStatusEmitter: TaskStatusEmitter,
  taskStatusObservables: TaskStatusObservables)
    extends OfferMatcherModule {

  private[this] lazy val offerMatcherMultiplexer: ActorRef = {
    val props = OfferMatcherMultiplexerActor.props(
      random,
      clock,
      taskStatusEmitter)
    val actorRef = leadershipModule.startWhenLeader(props, "OfferMatcherMultiplexer")

    taskStatusObservables.forAll.foreach {
      case TaskStatusUpdate(_, _, MarathonTaskStatus.Staging(_)) =>
        actorRef ! OfferMatcherMultiplexerActor.AddTaskLaunchTokens(1)
      case _ => // ignore
    }
    actorRef
  }

  override val globalOfferMatcher: OfferMatcher = new ActorOfferMatcher(clock, offerMatcherMultiplexer)

  override val subOfferMatcherManager: OfferMatcherManager = new ActorOfferMatcherManager(offerMatcherMultiplexer)
}
