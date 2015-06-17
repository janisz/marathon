package mesosphere.marathon.core.leadership.impl

import akka.actor.{ Status, Terminated, PoisonPill, Stash, ActorRef, Actor, ActorLogging, Props }
import akka.event.LoggingReceive
import mesosphere.marathon.api.LeaderInfo
import mesosphere.marathon.event.LocalLeadershipEvent

private[impl] object WhenLeaderActor {
  def props(leaderInfo: LeaderInfo, childProps: Props, childName: String): Props = {
    Props(
      new WhenLeaderActor(leaderInfo, childProps, childName)
    )
  }
}

/**
  * Wraps an actor which is only started when we are currently the leader.
  */
private class WhenLeaderActor(leaderInfo: LeaderInfo, childProps: Props, childName: String)
    extends Actor with ActorLogging with Stash {

  override def preStart(): Unit = {
    leaderInfo.subscribe(self)
  }

  override def postStop(): Unit = {
    leaderInfo.unsubscribe(self)
  }

  override def receive: Receive = suspended

  private[this] def suspended: Receive = LoggingReceive.withLabel("suspended") {
    case LocalLeadershipEvent.ElectedAsLeader =>
      val childRef = context.actorOf(childProps, childName)
      context.become(active(childRef))

    case LocalLeadershipEvent.Standby => // nothing changes

    case unhandled: Any               => sender() ! Status.Failure(new IllegalStateException("not currently leader"))
  }

  private[this] def active(childRef: ActorRef): Receive = LoggingReceive.withLabel("active") {
    case LocalLeadershipEvent.ElectedAsLeader => // nothing changes

    case LocalLeadershipEvent.Standby =>
      childRef ! PoisonPill
      context.watch(childRef)
      context.become(dying(childRef))

    case unhandled: Any => childRef.forward(unhandled)
  }

  private[this] def dying(childRef: ActorRef): Receive = LoggingReceive.withLabel("dying") {
    case Terminated(`childRef`) =>
      unstashAll()
      context.become(suspended)

    case unhandled: Any => stash()
  }
}
