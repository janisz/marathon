package mesosphere.util.state

import java.util.concurrent.TimeUnit

import mesosphere.marathon.MarathonSpec
import mesosphere.marathon.integration.setup.{ IntegrationFunSuite, StartedZookeeper }
import mesosphere.util.state.mesos.MesosStateStore
import org.apache.mesos.Protos.MasterInfo
import org.apache.mesos.state.ZooKeeperState
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures._

import scala.concurrent.duration._

class ZkMesosMasterUtilTest extends IntegrationFunSuite with StartedZookeeper with MarathonSpec with GivenWhenThen
    with Matchers {

  test("read mesos master from zk"){
    Given("zk contains mesos masterInfo value")
    val masterInfo = MasterInfo.newBuilder()
      .setHostname("localhost")
      .setPort(2890).build()
    mesosStore.create("info_0001", masterInfo.toByteArray).futureValue
    val conf = makeConfig(
      "--master", config.zkPath
    )
    val masterUtil = MesosMasterUtil(conf)

    When("read config from zk")

    val masterUrl = masterUtil.masterUrl

    Then("")
    val expected = "http://localhost:2890"
    masterUrl should equal(expected)

  }

  lazy val mesosStore: MesosStateStore = {
    val duration = 30.seconds
    val state = new ZooKeeperState(
      config.zkHostAndPort,
      duration.toMillis,
      TimeUnit.MILLISECONDS,
      config.zkPath
    )
    new MesosStateStore(state, duration)
  }
  override protected def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap + ("zkPort" -> "2185"))
  }

}
