package mesosphere.marathon.core.leadership.impl

import akka.actor.{ ActorRefFactory, ActorRef, Props }
import mesosphere.marathon.api.LeaderInfo
import mesosphere.marathon.core.leadership.LeadershipModule

private[leadership] class DefaultLeaderShipModule(actorRefFactory: ActorRefFactory, leaderInfo: LeaderInfo)
    extends LeadershipModule {
  private[this] val nextName: () => String = {
    var serial: Int = -1
    () => {
      serial += 1
      s"whenLeader$serial"
    }
  }

  override def startWhenLeader(props: Props, name: String): ActorRef = {
    val proxyProps = WhenLeaderActor.props(leaderInfo, props, name)
    actorRefFactory.actorOf(proxyProps, nextName())
  }
}
