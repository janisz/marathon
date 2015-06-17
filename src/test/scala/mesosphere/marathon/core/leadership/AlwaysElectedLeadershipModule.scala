package mesosphere.marathon.core.leadership

import akka.actor.{ ActorRefFactory, ActorRef, Props }
import mesosphere.marathon.core.base.ShutdownHooks
import mesosphere.marathon.core.base.actors.ActorsModule

private class AlwaysElectedLeadershipModule(actorRefFactory: ActorRefFactory) extends LeadershipModule {
  override def startWhenLeader(props: Props, name: String): ActorRef = actorRefFactory.actorOf(props, name)
}

object AlwaysElectedLeadershipModule {
  def apply(shutdownHooks: ShutdownHooks): LeadershipModule = {
    val actorsModule = ActorsModule(shutdownHooks)
    new AlwaysElectedLeadershipModule(actorsModule.actorRefFactory)
  }
}
