package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.Sitemap
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SitemapEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)
    val outpath = args(1)
    val sitemapUrlPrefix = args(2)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Sitemap")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    Sitemap.execute(spark: SparkSession, inpath, outpath, sitemapUrlPrefix)

    spark.stop()
  }
}
