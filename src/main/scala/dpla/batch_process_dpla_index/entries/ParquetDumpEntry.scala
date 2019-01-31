package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.ParquetDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for generating a full dump of the DPLA ElasticSearch index
  * in parquet.
  *
  * Args
  *   args(0) = outpath   The full local or S3 output path, ending in ".parquet"
  *                       e.g. s3a://dpla-provider-export/2019/01/all.parquet
  *
  *   args(1) = query     Optional parameters for an ElasticSearch query,
  *                       e.g. ?q=hamster
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
