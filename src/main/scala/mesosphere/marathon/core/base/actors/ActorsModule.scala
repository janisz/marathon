package mesosphere.marathon.core.base.actors

import akka.actor.{ Scheduler, ActorRefFactory, ActorSystem }
import mesosphere.marathon.core.base.ShutdownHooks
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

/**
  * Contains basic dependencies used throughout the application disregarding the concrete function.
  */
trait ActorsModule {
  def actorRefFactory: ActorRefFactory
}

object ActorsModule {
  def apply(shutdownHooks: ShutdownHooks): ActorsModule = new DefaultActorsModule(shutdownHooks)
}

private class DefaultActorsModule(shutdownHooks: ShutdownHooks) extends ActorsModule {
  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] lazy val actorSystem = ActorSystem()

  override def actorRefFactory: ActorRefFactory = actorSystem

  shutdownHooks.onShutdown {
    log.info("Shutting down actor system")
    actorSystem.shutdown()
    actorSystem.awaitTermination(10.seconds)
  }
}
