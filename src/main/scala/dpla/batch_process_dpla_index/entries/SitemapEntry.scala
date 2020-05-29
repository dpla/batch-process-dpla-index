package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.Sitemap
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating DPLA sitemaps.
  * Files are compressed for S3 writes (local writes are not compressed).
  *
  * Args
  *   args(0) = inpath    Local or S3 path to a parquet dump of the DPLA index.
  *
  *   args(1) = outpath   Local or S3 output path to the top-level directory.
  *                       Date/timestamps will be added to the auto-generated file paths.
  *                       e.g. s3a://sitemaps.dp.la/
  *
  *   args(2) = sitemapUrlPrefix   e.g. http://sitemaps.dp.la/
  *
  * A spark-submit invocation requires the following packages:
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  *
  *   Double-check build file for correct package versions
  */

object SitemapEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)
    val outpath = args(1)
    val sitemapUrlPrefix = args(2)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Sitemap")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    Sitemap.execute(spark, inpath, outpath, sitemapUrlPrefix)

    spark.stop()
  }
}
