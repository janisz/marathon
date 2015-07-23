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
      .setId("00000-0000")
      .setIp(127)
      .setHostname("localhost")
      .setPort(5050).build()
    mesosStore.create("info_0000", masterInfo.toByteArray).futureValue
    val conf = makeConfig(
      "--master", config.zk
    )
    val masterUtil = MesosMasterUtil(conf)

    When("read config from zk")

    val masterUrl = masterUtil.masterUrl

    Then("masterUrl should be same as stored in zk")
    val expected = "http://localhost:5050"
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
