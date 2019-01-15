package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.{JsonlDump, JsonlDumpExp}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JsonlDumpEntry {
  def main(args: Array[String]): Unit = {

    val outpath: String = args(0)
    val query: String = args.lift(1).getOrElse("") // e.g. "?q=hamster"

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: JSON-L Dump")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    JsonlDumpExp.execute(spark, outpath, query)

    spark.stop()
  }
}
