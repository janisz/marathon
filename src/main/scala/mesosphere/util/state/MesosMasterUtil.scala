package mesosphere.util.state

import com.twitter.util.Await
import com.twitter.zk.ZkClient
import org.apache.mesos.Protos.MasterInfo

/**
  * Utility class for keeping track of a Mesos MasterInfo
  */
trait MesosMasterUtil {

  def getMasterUrl: String
}

class ConstMesosMasterUtil(val masterUrl: String) extends MesosMasterUtil {
  override def getMasterUrl: String = masterUrl
}

class ZkMesosMasterUtil(zkClient: ZkClient, path : String) extends MesosMasterUtil {

  override def getMasterUrl: String = {
    val q = Await.result(zkClient(path).getChildren.apply()).children.head.getData.apply()
    val masterInfo = MasterInfo.parseFrom(Await.result(q).bytes)
    s"http://${masterInfo.getHostname}:${masterInfo.getPort}"
  }
}


