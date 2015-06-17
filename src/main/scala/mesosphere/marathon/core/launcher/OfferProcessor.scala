package mesosphere.marathon.core.launcher

import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.Offer

trait OfferProcessor {
  def processOffer(deadline: Timestamp, offer: Offer)
}
