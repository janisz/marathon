package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationFunTest
import mesosphere.marathon.integration.setup.EmbeddedMarathonTest

@IntegrationTest
class EventStreamIntegrationTest extends AkkaIntegrationFunTest with EmbeddedMarathonTest {

  before(cleanUp())

  test("adding an event subscriber") {

    When("read event stream without filters")
    val defaultClient = marathon.events()

    When("an filtered event subscriber is added")
    val filteredClient = marathon.events("non_existing_event_type")

    //TODO(janisz) delete when SSE support appear
    When("marathon is stopped")
    marathonServer.stop()

    Then("a notification should be sent to default subscribers")
    defaultClient.entityString.contains("type")
    And("not filtered")
    filteredClient.entityString.contains("type")
  }
}
