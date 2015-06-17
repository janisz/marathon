package mesosphere.marathon.core.matcher.impl

import akka.actor.ActorRef
import akka.util.Timeout
import mesosphere.marathon.core.matcher.{ OfferMatcher, OfferMatcherManager }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import akka.pattern.ask

private[matcher] object ActorOfferMatcherManager {
  sealed trait ChangeMatchersRequest
  case class AddMatcher(consumer: OfferMatcher) extends ChangeMatchersRequest
  case class RemoveMatcher(consumer: OfferMatcher) extends ChangeMatchersRequest

  sealed trait ChangeConsumersResponse
  case class MatcherAdded(consumer: OfferMatcher) extends ChangeConsumersResponse
  case class MatcherRemoved(consumer: OfferMatcher) extends ChangeConsumersResponse
}

private[matcher] class ActorOfferMatcherManager(actorRef: ActorRef) extends OfferMatcherManager {

  private[this] implicit val timeout: Timeout = 2.seconds

  override def addOfferMatcher(offerMatcher: OfferMatcher)(implicit ec: ExecutionContext): Future[Unit] = {
    val future = actorRef ? ActorOfferMatcherManager.AddMatcher(offerMatcher)
    future.map(_ => ())
  }

  override def removeOfferMatcher(offerMatcher: OfferMatcher)(implicit ec: ExecutionContext): Future[Unit] = {
    val future = actorRef ? ActorOfferMatcherManager.RemoveMatcher(offerMatcher)
    future.map(_ => ())
  }
}
