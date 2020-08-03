package dpla.batch_process_dpla_index.helpers

import java.io.InputStream

import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.Serialization
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody, Response}
import org.apache.commons.io.IOUtils

import scala.io.Source

class Index(
             val host: String,
             val port: String,
             val indexName: String,
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
    try
      if (!response.isSuccessful) {
        throw new RuntimeException(
          s"FAILED request for ${request.url.toString} (${response.code}, " +
            s"${response.message})"
        )
      }
    finally
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

    try
      if (!replResponse.isSuccessful) {
        throw new RuntimeException(
          s"FAILED request for ${replRequest.url.toString} " +
            s"(${replResponse.code}, ${replResponse.message})"
        )
      }
    finally
      replResponse.close()
  }

  def deleteIndex(): Unit =  {
    val request: Request =
      new Request.Builder()
        .url(s"http://$host:$port/$indexName")
        .delete()
        .build()
    val response: Response = httpClient.newCall(request).execute()

    try {
      if (!response.isSuccessful) {
        throw new RuntimeException(
          s"FAILED request for ${request.url.toString} (${response.code}, " +
            s"${response.message})"
        )
      }
      System.out.println(s"Deleted $indexName")
    }
    finally
      response.close()
  }

  // Set alias for this index.
  def deploy(alias: String): Unit = {
    val oldIndices: Set[String] = getAliasedIndices(alias)
    setExclusiveAlias(alias, oldIndices)
  }

  private def getAliasedIndices(alias: String): Set[String] = {
    implicit val formats = DefaultFormats

    val request: Request =
      new Request.Builder()
        .url(s"http://$host:$port/_alias/$alias")
        .get()
        .build()
    val response: Response = httpClient.newCall(request).execute()

    val indices: Set[String] =
      try
        if (!response.isSuccessful)
          if (response.code != 404)
            // An unexpected error has occurred
            throw new RuntimeException(
              s"FAILED request for ${request.url.toString} (${response.code}, " +
                s"${response.message})"
            )
          else {
            // A 404 error indicates that there is no existing index with the given alias, return empty set.
            System.out.println(s"No indices with the alias $alias were found.")
            Set[String]()
          }
        else {
          // Get the names of all indices with the given alias
          val iNames = JsonMethods.parse(response.body.string).extract[Map[String, Any]].keySet
          System.out.println(s"Found indices with the alias $alias:")
          iNames.foreach(System.out.println)
          iNames
        }
      finally
        response.close()

    indices
  }

  // Add the alias to this index and remove this alias from any old indices.
  private def setExclusiveAlias(alias: String, oldIndices: Set[String]): Unit = {
    implicit val formats = org.json4s.DefaultFormats
    val mt: MediaType = MediaType.parse("application/json")

    val add = Map("add" -> Map("index" -> indexName, "alias" -> alias))
    val remove = oldIndices.toSeq.map(i =>
      Map("remove" -> Map("index" -> i, "alias" -> alias))
    )
    val actions = Map("actions" -> (remove :+ add).toList)
    val postBody = Serialization.write(actions)

    val reqBody: RequestBody = RequestBody.create(mt, postBody)

    val request: Request =
      new Request.Builder()
        .url(s"http://$host:$port/_aliases")
        .post(reqBody)
        .build()
    val response: Response = httpClient.newCall(request).execute()

    try
      if (!response.isSuccessful) {
        throw new RuntimeException(
          s"FAILED request for ${request.url.toString} ${request.body.toString} (${response.code}, " +
            s"${response.message})"
        )
      }
    finally
      response.close()
  }
}
