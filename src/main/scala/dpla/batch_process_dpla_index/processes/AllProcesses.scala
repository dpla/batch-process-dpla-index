package dpla.batch_process_dpla_index.processes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for executing all processes.
  *
  *   args(0) = parquetOut    Local or S3 path to the top-level directory destination.
  *                           Month and year will be added to the auto-generated file paths.
  *                           e.g. s3a://dpla-provider-export/
  *
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
  *
  * A spark-submit invocation requires the following packages:
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.7
  *   com.squareup.okhttp3:okhttp:3.8.0
  *
  *   Double-check build file for correct package versions
  */

object AllProcesses {

  def main(args: Array[String]): Unit = {
    val inBucket = args(0)
    val outBucketParquet = args(1)
    val outBucketJsonl = args(2)
    val outBucketMq = args(3)
    val outBucketSitemap = args(4)
    val sitemapUrlPrefix = args(5)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: All processes")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val parquetPath = ParquetDump.execute(spark, inBucket, outBucketParquet)
    JsonlDump.execute(spark, inBucket, outBucketJsonl)
    MqReports.execute(spark, parquetPath, outBucketMq)
    Sitemap.execute(spark, parquetPath, outBucketSitemap, sitemapUrlPrefix)

    spark.stop()
  }
}
