package mesosphere.marathon.core.launchqueue.impl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import mesosphere.marathon.MarathonSpec
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.state.{Timestamp, AppDefinition}
import mesosphere.marathon.state.PathId._
import org.scalatest.Matchers

import scala.concurrent.duration._

class RateLimiterTest extends TestKit(ActorSystem("system")) with MarathonSpec with Matchers {
  val clock = ConstantClock(Timestamp.now())

  // TODO: DecreasDelay

  test("addDelay") {
    val limiter = new RateLimiter(clock)
    val app = AppDefinition(id = "test".toPath, backoff = 10.seconds)

    limiter.addDelay(app)

    limiter.getDelay(app) should be(clock.now() + 10.seconds)
  }

  test("resetDelay") {
    val limiter = new RateLimiter(clock)
    val app = AppDefinition(id = "test".toPath, backoff = 10.seconds)

    limiter.addDelay(app)

    limiter.resetDelay(app)

    limiter.getDelay(app) should be(clock.now())
  }

}
