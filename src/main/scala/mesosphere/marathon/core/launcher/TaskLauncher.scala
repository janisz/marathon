package mesosphere.marathon.core.launcher

import org.apache.mesos.Protos.{ OfferID, TaskInfo }

trait TaskLauncher {
  def launchTasks(offerID: OfferID, taskInfos: Seq[TaskInfo])
  def declineOffer(offerID: OfferID)
}
