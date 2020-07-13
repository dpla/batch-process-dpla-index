package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.ParquetDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating a parquet dump of the DPLA ElasticSearch index.
  *
  * Args
  *   args(0) = inpath   Local or S3 path to the necropolis data dump.
  *                      e.g. s3a://dpla-necropolis/2020/07/tombstones.parquet
  *
  * A spark-submit invocation requires the following packages:
  *   org.elasticsearch:elasticsearch-spark-20_2.11:7.3.2
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  *
  *   Double-check build file for correct package versions
  */

object NecroIndexEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Parquet Dump")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    NecroIndex.execute(spark, inpath)

    spark.stop()
  }
}
