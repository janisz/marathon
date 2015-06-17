package mesosphere.marathon.core.matcher

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.impl.DefaultOfferMatcherModule
import mesosphere.marathon.core.task.bus.{ TaskStatusEmitter, TaskStatusObservables }

import scala.util.Random

trait OfferMatcherModule {
  /** The offer matcher which forwards match requests to all registered sub offer matchers. */
  def globalOfferMatcher: OfferMatcher
  def subOfferMatcherManager: OfferMatcherManager
}

object OfferMatcherModule {
  def apply(
    clock: Clock,
    random: Random,
    leadershipModule: LeadershipModule,
    taskStatusEmitter: TaskStatusEmitter,
    taskStatusObservables: TaskStatusObservables): OfferMatcherModule =
    new DefaultOfferMatcherModule(clock, random, leadershipModule, taskStatusEmitter, taskStatusObservables)
}

