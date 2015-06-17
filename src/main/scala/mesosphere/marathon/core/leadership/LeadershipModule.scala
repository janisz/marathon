package mesosphere.marathon.core.leadership

import akka.actor.{ ActorRefFactory, ActorRef, Props }
import mesosphere.marathon.api.LeaderInfo
import mesosphere.marathon.core.leadership.impl.DefaultLeaderShipModule

trait LeadershipModule {
  /**
    * Starts the given top-level actor when we are leader. Otherwise reject all messages
    * to the actor by replying with Failure messages.
    *
    * The returned ActorRef stays stable across leadership changes.
    */
  def startWhenLeader(props: Props, name: String): ActorRef
}

object LeadershipModule {
  def apply(actorRefFactory: ActorRefFactory, leaderInfo: LeaderInfo): LeadershipModule = {
    new DefaultLeaderShipModule(actorRefFactory, leaderInfo)
  }
}
