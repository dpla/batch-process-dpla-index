package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for executing all processes.
  *
  * Args
  *   args(0) = parquetOut    Local or S3 path to the top-level directory destination.
  * *                         Month and year will be added to the auto-generated file paths.
  * *                         e.g. s3a://dpla-provider-export/
  * *
  *   args(1) = jsonlOut      Local or S3 path to the top-level directory destination.
  *                           Month and year will be added to the auto-generated file paths.
  *                           e.g. s3a://dpla-provider-export/
  *
  *   args(2) = mqOut         Local or S3 path to the top-level directory destination.
  *                           Month and year will be added to the auto-generated files paths.
  *                           e.g. s3a://dashboard-analytics/
  *
  *   args(3) = sitemapOut    Local or S3 output path to the top-level directory.
  *                           Date/timestamps will be added to the auto-generated file paths.
  *                           e.g. s3a://sitemaps.dp.la/
  *
  *   args(4) = sitemapUrlPrefix   e.g. http://sitemaps.dp.la/
  *
  *   args(5) = tombstoneOut  Local or S3 output path to the top-level directory destination.
  *                           Month and year will be added to the auto-generated files paths.
  *                           e.g. s3a://dpla-necropolis/
  *
  *   args(6) = doNecro       Boolean, default is false.
  *                           Set to true if you want to compute necropolis data and index to search.internal.dp.la
  *                           This process is too intensive to run locally.
  *
  *   args(7) = query         Optional parameters for an ElasticSearch query,
  *                           e.g. ?q=hamster
  *
  * A spark-submit invocation requires the following packages:
  *   org.elasticsearch:elasticsearch-spark-20_2.11:7.3.2
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  *   com.squareup.okhttp3:okhttp:3.8.0
  *
  *   Double-check build file for correct package versions
  */

object AllProcessesEntry {

  def main(args: Array[String]): Unit = {

    val parquetOut = args(0)
    val jsonlOut = args(1)
    val mqOut = args(2)
    val sitemapOut = args(3)
    val sitemapUrlPrefix = args(4)
    val tombstoneOut = args(5)
    val doNecro = args.lift(6).getOrElse("false").toBoolean
    val query = args.lift(7).getOrElse("")

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: All processes")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    val parquetPath = ParquetDump.execute(spark, parquetOut, query)
//    JsonlDump.execute(spark, jsonlOut)
//    MqReports.execute(spark, parquetPath, mqOut)
//    Sitemap.execute(spark, parquetPath, sitemapOut, sitemapUrlPrefix)

    if (doNecro){
      // TODO Double check that these are good default values - should any be parameterized?
      val esClusterHost = "search.internal.dp.la"
      val esPort = "9200"
      val alias = "necropolis" // all existing indices with this alias will be deleted
      val shards = 3
      val replicas = 1

      val parquetPath = "s3a://dpla-provider-export/2020/07/all.parquet"
      val necroPath = NecroData.execute(spark, parquetPath, tombstoneOut, None)
      NecroIndex.execute(spark, necroPath, esClusterHost, esPort, alias, shards, replicas)
    }
    
    spark.stop()
  }
}
