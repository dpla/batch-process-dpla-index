package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.ParquetDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
