package dpla.batch_process_dpla_index.processes

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

    println(
      f"""
         |Writing tombstones
         |From $inpath
         |To $esClusterHost:$esPort/$indexName
         |Creating $shards shards and $replicas replicas
      """.stripMargin)

    println("Setting up.")
    val client: OkHttpClient = buildHttpClient
    val index = new Index(esClusterHost, esPort, indexName, shards, replicas, client)

    println("Creating index.")
    index.createIndex()

    println("Saving.")
    ElasticSearchWriter.saveRecords(indexName, spark, inpath, esClusterHost, esPort)

    println("Enabling replicas.")
    index.createReplicas()

    println("Done.")

    val duration: String = ((System.currentTimeMillis() - start) / 60000.0).toString
    println(s"Runtime: $duration minutes.")
  }

  private def buildHttpClient =
    new OkHttpClient.Builder()
      .connectTimeout(5, TimeUnit.SECONDS)
      .retryOnConnectionFailure(false)
      .followRedirects(false)
      .build()
}
