package mesosphere.marathon.core.launchqueue.impl

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.state.{ AppDefinition, PathId }
import LaunchQueue.QueuedTaskCount

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration.{ Deadline, _ }
import scala.util.control.NonFatal

private[impl] class ActorLaunchQueue(actorRef: ActorRef) extends LaunchQueue {
  override def list: Seq[QueuedTaskCount] = askActor("list")(ActorLaunchQueue.List).asInstanceOf[Seq[QueuedTaskCount]]
  override def count(appId: PathId): Int =
    askActor("count")(ActorLaunchQueue.Count(appId))
      .asInstanceOf[Option[QueuedTaskCount]].map(_.tasksLeftToLaunch).getOrElse(0)
  override def listApps: Seq[AppDefinition] = list.map(_.app)
  override def listWithDelay: Seq[(QueuedTaskCount, Deadline)] =
    list.map(_ -> Deadline.now) // TODO: include rateLimiter
  override def purge(appId: PathId): Unit = askActor("purge")(ActorLaunchQueue.Purge(appId))
  override def add(app: AppDefinition, count: Int): Unit = askActor("add")(ActorLaunchQueue.Add(app, count))

  private[this] def askActor[T](method: String)(message: T): Any = {
    implicit val timeout: Timeout = 1.second
    val answerFuture = actorRef ? message
    import scala.concurrent.ExecutionContext.Implicits.global
    answerFuture.recover {
      case NonFatal(e) => throw new RuntimeException(s"in $method", e)
    }
    Await.result(answerFuture, 1.second)
  }

  override def resetDelay(appId: PathId): Unit = ??? // FIXME: Implement
}

private[impl] object ActorLaunchQueue {
  sealed trait Request
  case object List extends Request
  case class Count(appId: PathId) extends Request
  case class Purge(appId: PathId) extends Request
  case object ConfirmPurge extends Request
  case class Add(app: AppDefinition, count: Int) extends Request
}
