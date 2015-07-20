package mesosphere.util.state

/**
  * Utility class for keeping track of a Mesos MasterInfo
  */
class MesosMasterUtil {

  var mesosMasterUrl: String = _

  //TODO: Do it better and keep value in zk
  def store(host: String, port: Int): String = {
    mesosMasterUrl = s"http://$host:$port"
    mesosMasterUrl
  }

  def load(): String = {
    mesosMasterUrl
  }
}
