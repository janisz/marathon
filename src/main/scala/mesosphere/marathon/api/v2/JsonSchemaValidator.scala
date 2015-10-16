package mesosphere.marathon.api.v2

import com.github.fge.jackson.JsonLoader
import com.github.fge.jsonschema.core.report.ListProcessingReport
import com.github.fge.jsonschema.main.{ JsonSchema, JsonSchemaFactory }
import play.api.libs.json.{ JsObject, JsNull, JsValue }

case class JsonSchemaValidationException(msg: String, report: ListProcessingReport) extends Exception

class JsonSchemaValidator {

  private val factory = JsonSchemaFactory.byDefault()

  private lazy val appSchema = loadSchema("/public/api/v2/schema/AppDefinition.json")

  def validateApp(appDefinition: JsValue): Unit = {
    val app = JsObject(appDefinition.asInstanceOf[JsObject].fields.filter(t => withoutValue(t._2)))
    val appJson = JsonLoader.fromString(app.toString)
    val report = appSchema.validate(appJson)
    if (!report.isSuccess) {
      throw new JsonSchemaValidationException("Invalid JSON", report.asInstanceOf[ListProcessingReport])
    }
  }

  private def withoutValue(v: JsValue) = v match {
    case JsNull => false
    case _      => true
  }

  private def loadSchema(schemaPath: String): JsonSchema = {
    val appDefinition = JsonLoader.fromResource(schemaPath)
    factory.getJsonSchema(appDefinition)
  }
}
