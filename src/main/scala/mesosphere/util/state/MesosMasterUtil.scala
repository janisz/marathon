package mesosphere.util.state

import com.twitter.util.Await
import com.twitter.zk.{ NativeConnector, AuthInfo, ZkClient }
import mesosphere.marathon.MarathonConf
import org.apache.mesos.Protos.MasterInfo
import org.apache.zookeeper.ZooDefs.Ids

/**
  * Utility class for keeping track of a Mesos MasterInfo
  */
trait MesosMasterUtil {

  def masterUrl: String
}

object MesosMasterUtil {
  val filePattern = s"""^file:.*$$""".r

  val userName = """[^/@:]+"""
  val password = """[^/@:]+"""
  val credentials = s"($userName):($password)@"
  val hostAndPort = """[A-z0-9-.]+(?::\d+)?"""
  val zkNode = """[^/]+"""
  val zkURLPattern = s"""^zk://(?:$credentials)?($hostAndPort(?:,$hostAndPort)*)(/$zkNode(?:/$zkNode)*)$$""".r

  def apply(conf: MarathonConf): MesosMasterUtil = {

    def create(mesosMasterParam: String): MesosMasterUtil = mesosMasterParam match {
      case zkURLPattern(username, password, hosts, path) => {
        import scala.collection.JavaConverters._
        import com.twitter.util.TimeConversions._
        implicit val timer = com.twitter.util.Timer.Nil
        val sessionTimeout = conf.zooKeeperSessionTimeout.get.map(_.millis).getOrElse(30.minutes)
        val authInfo = for {
          uName <- Option(username)
          pass <- Option(password)
          authInfo <- Some(AuthInfo.digest(uName, pass))
        } yield authInfo
        val connector = NativeConnector(hosts, None, sessionTimeout, timer, authInfo)
        val client = ZkClient(connector)
          .withAcl(Ids.OPEN_ACL_UNSAFE.asScala)
          .withRetries(3)
        new ZkMesosMasterUtil(client, path)
      }
      case filePattern() => create(scala.io.Source.fromURL(mesosMasterParam).mkString)
      case _             => ConstMesosMasterUtil(mesosMasterParam)
    }
    create(conf.mesosMaster())
  }

}

case class ConstMesosMasterUtil(masterUrl: String) extends MesosMasterUtil

class ZkMesosMasterUtil(zkClient: ZkClient, path: String) extends MesosMasterUtil {

  override def masterUrl: String = {
    val children = Await.result(zkClient(path).getChildren()).children
    val infoChildren = children.filter(x => x.path.contains("info_"))
    val firstInfoChildrenData = infoChildren.head.getData()
    val masterInfo = MasterInfo.parseFrom(Await.result(firstInfoChildrenData).bytes)
    s"http://${masterInfo.getHostname}:${masterInfo.getPort}"
  }
}

