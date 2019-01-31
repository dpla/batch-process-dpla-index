package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.JsonlDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating public JSONL dumps of the DPLA index.
  * The source is the most recent JSONL dump in s3://dpla-master-dataset
  * This creates dump with all items, along with individual dumps for each providers.
  *
  * Args
  *   args(0) = outpath   Local or S3 output path to the top-level directory that will contain
  *                       all.jsonl and the individual provider dumps.
  *                       Month and year will be added to the auto-generated file paths.
  *                       e.g. s3a://dpla-provider-export/
  */

object JsonlDumpEntry {
  def main(args: Array[String]): Unit = {

    val outpath: String = args(0)

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: JSON-L Dump")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    JsonlDump.execute(spark, outpath)

    spark.stop()
  }
}
