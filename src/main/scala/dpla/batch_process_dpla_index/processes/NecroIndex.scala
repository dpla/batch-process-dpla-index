package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{ElasticSearchWriter, Index}
import org.apache.spark.sql.SparkSession
import okhttp3.OkHttpClient
import java.util.concurrent.TimeUnit

object NecroIndex {

  def execute(spark: SparkSession,
              inpath: String,
              esClusterHost: String,
              esPort: String,
              indexName: String,
              shards: Int,
              replicas: Int): Unit = {

    // start is used to calculate runtime
    val start = System.currentTimeMillis()

    // dateTime is used to create a timestamp for the index name
    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val timestamp: String = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))

    val timestampedIndexName = indexName + "-" + timestamp

    println(
      f"""
         |Writing tombstones
         |From $inpath
         |To $esClusterHost:$esPort/$timestampedIndexName
         |Alias $esClusterHost:$esPort/$indexName
         |Creating $shards shards and $replicas replicas
      """.stripMargin)

    println("Setting up.")
    val client: OkHttpClient = buildHttpClient
    val index = new Index(esClusterHost, esPort, timestampedIndexName, shards, replicas, client)

    println(s"Creating index $timestampedIndexName")
    index.createIndex()

    println("Saving.")
    ElasticSearchWriter.saveRecords(timestampedIndexName, spark, inpath, esClusterHost, esPort)

    println("Enabling replicas.")
    index.createReplicas()

    println(s"Deploying $timestampedIndexName to alias $indexName")
    index.deploy(indexName)

    println("Done.")

    val duration: String = ((System.currentTimeMillis() - start) / 60000.0).toString
    println(s"Necropolis index runtime: $duration minutes.")
  }

  private def buildHttpClient =
    new OkHttpClient.Builder()
      .connectTimeout(5, TimeUnit.SECONDS)
      .retryOnConnectionFailure(false)
      .followRedirects(false)
      .build()
}
