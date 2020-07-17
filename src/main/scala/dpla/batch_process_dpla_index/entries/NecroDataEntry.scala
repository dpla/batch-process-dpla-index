package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.NecroData
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating a parquet dump of the ghost records.
  *
  * Args
  *   args(0) = outpath         Local or S3 path to the top-level directory destination.
  *                             Month and year will be added to the auto-generated file paths.
  *                             e.g. s3a://dpla-necropolis/
  *
  *   args(1) = newRecordsPath  Local or S3 path to the parquet file of records.
  *                             e.g. s3a://dpla-provider-export/2020/05/all.parquet
  *
  *   args(2) = previousDate    Optional
  *                             Date of the previous tombstones / item dataset for comparison with newRecordsPath
  *                             Default is one month prior to the date specified in newRecordsPath
  *
  * A spark-submit invocation requires the following packages:
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  *
  *   Double-check build file for correct package versions
  */

object NecroDataEntry {

  def main(args: Array[String]): Unit = {

    val outpath: String = args(0)
    val newRecordsPath: String = args(1)
    val previousDate: Option[String] = args.lift(2)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Generate Necropolis Data")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    NecroData.execute(spark, newRecordsPath, outpath, previousDate)

    spark.stop()
  }
}
