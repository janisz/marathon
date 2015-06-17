package mesosphere.marathon.core.matcher

import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.{ TaskInfo, OfferID, Offer }

import scala.concurrent.Future

object OfferMatcher {
  /**
    * Reply from an offer matcher to a MatchOffer. If the offer match
    * could not match the offer in any way it should simply leave the tasks
    * collection empty.
    *
    * To increase fairness between matchers, each normal matcher should only launch as
    * few tasks as possible per offer -- usually one. Multiple tasks could be used
    * if the tasks need to be colocated. The OfferMultiplexer tries to summarize suitable
    * matches from multiple offer matches into one response.
    *
    * A MatchedTasks reply does not guarantee that these tasks can actually be launched.
    * The launcher of message should setup some kind of timeout mechanism.
    *
    * The receiver of this message should send a DeclineLaunch message to the launcher though if they
    * are any obvious reasons to deny launching these tasks.
    */
  case class MatchedTasks(offerId: OfferID, tasks: Seq[TaskInfo])
}

/**
  * Tries to match offers with given tasks.
  */
trait OfferMatcher {
  def processOffer(deadline: Timestamp, offer: Offer): Future[MatchedTasks]
}
