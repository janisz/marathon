package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.core.launcher.{ TaskLauncher, OfferProcessor }
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.Offer

private[impl] class DefaultOfferProcessor(offerMatcher: OfferMatcher, taskLauncher: TaskLauncher)
    extends OfferProcessor {
  import scala.concurrent.ExecutionContext.Implicits.global

  override def processOffer(deadline: Timestamp, offer: Offer): Unit = {
    offerMatcher.processOffer(deadline, offer).map {
      case MatchedTasks(offerId, tasks) =>
        if (tasks.nonEmpty) {
          taskLauncher.launchTasks(offerId, tasks)
        }
        else {
          taskLauncher.declineOffer(offerId)
        }
    }
  }
}
