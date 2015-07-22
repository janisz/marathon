package mesosphere.util.state

import mesosphere.marathon.{ MarathonSpec, MarathonConf }
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.reflect.io.File

class MesosMasterUtilTest extends MarathonSpec with Matchers with GivenWhenThen {

  val underTest: MarathonConf => MesosMasterUtil = MesosMasterUtil.apply

  test("should create ConstMesosMasterUtil"){
    Given("marathonConf with mesos master in url format")
    val conf = makeConfig(
      "--master", "127.0.0.1:5050"
    )
    When("mesosMasterUtil apply invoked")
    val result = MesosMasterUtil(conf)

    Then("should return ConstMesosMasterUtil")
    val expected = new ConstMesosMasterUtil("127.0.0.1:5050")
    result should equal(expected)
  }

  test("should create ZkMesosMasterUtil"){
    Given("marathonConf with mesos master in zk url format")
    val conf = makeConfig(
      "--master", "zk://127.0.0.1:5050/mesos"
    )
    When("mesosMasterUtil apply invoked")
    val result = MesosMasterUtil(conf)

    Then("should return ZkMesosMasterUtil")
    result shouldBe a[ZkMesosMasterUtil]
  }

  test("should create ZkMesosMasterUtil by file with zk config"){
    Given("marathonConf with mesos master in file format")
    val tempFile = File.makeTemp()
    tempFile.writeAll("zk://127.0.0.1:5050/mesos")
    tempFile.toURL.toString

    val conf = makeConfig(
      "--master", tempFile.toURL.toString
    )
    When("mesosMasterUtil apply invoked")
    val result = MesosMasterUtil(conf)

    Then("Should return ZkMesosMasterUtil")
    result shouldBe a[ZkMesosMasterUtil]
  }

  test("should create ZkMesosMasterUtil by file with url config"){
    Given("marathonConf with mesos master in file format")
    val tempFile = File.makeTemp()
    tempFile.writeAll("127.0.0.1:5050")
    tempFile.toURL.toString

    val conf = makeConfig(
      "--master", tempFile.toURL.toString
    )
    When("mesosMasterUtil apply invoked")
    val result = MesosMasterUtil(conf)

    Then("should return ConstMesosMasterUtil")
    result shouldBe a[ConstMesosMasterUtil]
  }

}
