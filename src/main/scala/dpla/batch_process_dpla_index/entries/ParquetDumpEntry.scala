package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.ParquetDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating a parquet dump of the DPLA ElasticSearch index.
  *
  * Args
  *   args(0) = outpath   Local or S3 path to the top-level directory destination.
  * *                     Month and year will be added to the auto-generated file paths.
  * *                     e.g. s3a://dpla-provider-export/
  * *
  *   args(1) = query     Optional parameters for an ElasticSearch query,
  *                       e.g. ?q=hamster
  *
  * A spark-submit invocation requires the following packages:
  *   org.elasticsearch:elasticsearch-spark-20_2.11:7.3.2
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  *
  *   Double-check build file for correct package versions
  */

object ParquetDumpEntry {

  def main(args: Array[String]): Unit = {

    val outpath = args(0)
    val query = args.lift(1).getOrElse("")

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Parquet Dump")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    ParquetDump.execute(spark, outpath, query)

    spark.stop()
  }
}
