package mesosphere.marathon.core.launchqueue.impl

import java.util.concurrent.TimeUnit

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.state.{ AppDefinition, PathId, Timestamp }
import org.apache.log4j.Logger

import scala.concurrent.duration._

private[impl] class RateLimiter(clock: Clock) {
  import RateLimiter._

  private[this] var taskLaunchDelays = Map[(PathId, Timestamp), Delay]()

  def getDelay(app: AppDefinition): Timestamp =
    taskLaunchDelays.get(app.id -> app.version).map(_.deadline) getOrElse clock.now()

  def addDelay(app: AppDefinition): Timestamp = {
    setNewDelay(app) {
      case Some(delay) => Some(delay.increased(clock, app))
      case None        => Some(Delay(clock, app))
    }
  }

  def decreaseDelay(app: AppDefinition): Timestamp = {
    setNewDelay(app)(_.map(_.decreased(clock, app)))
  }

  private[this] def setNewDelay(app: AppDefinition)(calcDelay: Option[Delay] => Option[Delay]): Timestamp = {
    calcDelay(taskLaunchDelays.get(app.id -> app.version)) match {
      case Some(newDelay) =>
        import mesosphere.util.DurationToHumanReadable
        val timeLeft = (clock.now() until newDelay.deadline).toHumanReadable
        log.info(s"Task launch delay for [${app.id}] is now [$timeLeft]")
        taskLaunchDelays += ((app.id, app.version) -> newDelay)
        newDelay.deadline

      case None =>
        resetDelay(app)
        clock.now()
    }
  }

  def resetDelay(app: AppDefinition): Unit = {
    if (taskLaunchDelays contains (app.id -> app.version))
      log.info(s"Task launch delay for [${app.id} - ${app.version}}] reset to zero")
    taskLaunchDelays = taskLaunchDelays - (app.id -> app.version)
  }
}

private object RateLimiter {
  private val log = Logger.getLogger(getClass.getName)

  private object Delay {
    def apply(clock: Clock, app: AppDefinition): Delay = Delay(clock, app.backoff)
    def apply(clock: Clock, delay: FiniteDuration): Delay = Delay(clock.now() + delay, delay)
    def none = Delay(Timestamp(0), 0.seconds)
  }

  private case class Delay(
      deadline: Timestamp,
      delay: FiniteDuration) {

    def increased(clock: Clock, app: AppDefinition): Delay = {
      val newDelay: FiniteDuration =
        app.maxLaunchDelay min FiniteDuration((delay.toNanos * app.backoffFactor).toLong, TimeUnit.NANOSECONDS)
      Delay(clock, newDelay)
    }

    def decreased(clock: Clock, app: AppDefinition): Delay = {
      val newDelay: FiniteDuration = FiniteDuration((delay.toNanos / app.backoffFactor).toLong, TimeUnit.NANOSECONDS)
      val newCalculatedDeadline = clock.now + newDelay
      val newDeadline = if (newCalculatedDeadline < deadline) newCalculatedDeadline else deadline
      Delay(newDeadline, newDelay)
    }
  }
}
