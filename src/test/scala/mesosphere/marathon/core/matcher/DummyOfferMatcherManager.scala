package mesosphere.marathon.core.matcher

import scala.concurrent.{ Future, ExecutionContext }

class DummyOfferMatcherManager extends OfferMatcherManager {
  var offerMatchers = Vector.empty[OfferMatcher]

  override def addOfferMatcher(offerMatcher: OfferMatcher)(implicit ec: ExecutionContext): Future[Unit] = {
    Future.successful(offerMatchers :+= offerMatcher)
  }

  override def removeOfferMatcher(offerMatcher: OfferMatcher)(implicit ec: ExecutionContext): Future[Unit] = {
    Future.successful(offerMatchers = offerMatchers.filter(_ != offerMatcher))
  }
}
