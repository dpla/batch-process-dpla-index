package dpla.batch_process_dpla_index.helpers

import java.io.InputStream

import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import org.json4s.JValue
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody, Response}

import org.apache.commons.io.IOUtils

import scala.io.Source

class Index(
             host: String,
             port: String,
             indexName: String,
             shards: Int,
             replicas: Int,
             httpClient: OkHttpClient) {

  def createIndex(): Unit = {
    val settingsStream: InputStream =
      getClass.getResourceAsStream(
        "/elasticsearch/index-settings-and-mappings-necropolis.json"
      )
    val settingsString: String = Source.fromInputStream(settingsStream).mkString
    IOUtils.closeQuietly(settingsStream)
    // settingsTemplate contains index settings (under "settings" property) and
    // mappings (under "mappings" property). We'll replace "settings" ...
    val settingsTemplate: JValue = JsonMethods.parse(settingsString)
    val settings: JValue = settingsTemplate.replace(
      "settings" :: "index" :: Nil,
      ("number_of_shards" -> shards) ~ ("number_of_replicas" -> 0) ~
        ("refresh_interval" -> "30s") ~ ("max_result_window" -> 50000)
    )
    val putBody: String = JsonMethods.compact(JsonMethods.render(settings))
    val mt: MediaType = MediaType.parse("application/json")
    val reqBody: RequestBody = RequestBody.create(mt, putBody)
    val request: Request =
      new Request.Builder()
        .url(s"http://$host:$port/$indexName?include_type_name=true") // include_name_type will be removed in ES 8
        .put(reqBody)
        .build()
    val response: Response = httpClient.newCall(request).execute()
    if (!response.isSuccessful) {
      throw new RuntimeException(
        s"FAILED request for ${request.url.toString} (${response.code}, " +
          s"${response.message})"
      )
    }
    response.close()
  }

  def createReplicas(): Unit = {
    println("Creating index replicas ...")
    import JsonMethods._
    val replSettings = "index" -> ("number_of_replicas" -> replicas)
    val replPutBody = compact(render(replSettings))
    val replReqBody: RequestBody =
      RequestBody.create(MediaType.parse("application/json"), replPutBody)
    val replRequest: Request =
      new Request.Builder()
        .url(s"http://$host:$port/$indexName/_settings")
        .put(replReqBody)
        .build()
    val replResponse: Response = httpClient.newCall(replRequest).execute()
    if (!replResponse.isSuccessful) {
      throw new RuntimeException(
        s"FAILED request for ${replRequest.url.toString} " +
          s"(${replResponse.code}, ${replResponse.message})"
      )
    }
    replResponse.close()
  }
}