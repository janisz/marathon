package mesosphere.marathon.api.v2

import javax.inject.Inject
import javax.ws.rs._
import javax.ws.rs.core.{ MediaType, Response }

import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.api.RestResource
import mesosphere.marathon.api.v2.json.{ Formats, V2AppDefinition }
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.state.PathId._
import play.api.libs.json.Json

@Path("v2/queue")
@Consumes(Array(MediaType.APPLICATION_JSON))
class QueueResource @Inject() (
    launchQueue: LaunchQueue,
    val config: MarathonConf) extends RestResource {

  @GET
  @Timed
  @Produces(Array(MediaType.APPLICATION_JSON))
  def index(): Response = {
    import Formats._

    val queuedWithDelay = launchQueue.listWithDelay.map {
      case (task, delay) =>
        Json.obj(
          "app" -> V2AppDefinition(task.app),
          "count" -> task.tasksLeftToLaunch,
          "delay" -> Json.obj(
            "timeLeftSeconds" -> math.max(0, delay.timeLeft.toSeconds), //deadlines can be negative
            "overdue" -> delay.isOverdue()
          )
        )
    }

    ok(Json.obj("queue" -> queuedWithDelay).toString())
  }

  @DELETE
  @Path("""{appId:.+}/delay""")
  def resetDelay(@PathParam("appId") appId: String): Response = {
    val id = appId.toRootPath
    launchQueue.listWithDelay.find(_._1.app.id == id).map {
      case (task, deadline) =>
        launchQueue.resetDelay(task.app.id)
        noContent
    }.getOrElse(notFound(s"application $appId not found in task queue"))
  }
}
