package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.MqReports
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating metadata quality reports for use in the
  * DPLA Hub Analytics Dashboard.
  *
  * Args
  *   args(0) = inpath    Local or S3 path to a parquet dump of the DPLA index.
  *
  *   args(1) = outpath   Local or S3 path to the top-level directory destination.
  *                       Month and year will be added to the auto-generated files paths.
  *                       e.g. s3a://dashboard-analytics/
  *
  * A spark-submit invocation requires the following packages:
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  */

object MqReportsEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)
    val outpath = args(1)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: MQ Reports")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    MqReports.execute(spark, inpath, outpath)

    spark.stop()
  }
}
