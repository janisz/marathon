package mesosphere.util

import net.logstash.logback.argument.{ StructuredArgument, StructuredArguments }
import org.slf4j.LoggerFactory

trait Logging {
  protected[this] val log = LoggerFactory.getLogger(getClass.getName)
}

object StructuredLogging {
  def v(key: String, value: Any): StructuredArgument = {
    StructuredArguments.value(key, String.valueOf(value))
  }
}
