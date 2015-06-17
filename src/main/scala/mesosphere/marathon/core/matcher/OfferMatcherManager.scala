package mesosphere.marathon.core.matcher

import scala.concurrent.{ ExecutionContext, Future }

trait OfferMatcherManager {
  def addOfferMatcher(offerMatcher: OfferMatcher)(implicit ec: ExecutionContext): Future[Unit]
  def removeOfferMatcher(offerMatcher: OfferMatcher)(implicit ec: ExecutionContext): Future[Unit]
}
