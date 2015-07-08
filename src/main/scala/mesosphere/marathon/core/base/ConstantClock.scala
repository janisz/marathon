package mesosphere.marathon.core.base

import mesosphere.marathon.state.Timestamp

case class ConstantClock(now_ : Timestamp) extends Clock {
  def now(): Timestamp = now_
}
