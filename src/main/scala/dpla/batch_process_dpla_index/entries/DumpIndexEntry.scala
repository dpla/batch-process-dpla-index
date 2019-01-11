package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.DumpIndex
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DumpIndexEntry {

  def main(args: Array[String]): Unit = {

    val outpath = args(0)
    val query = args.lift(1).getOrElse("")

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Dump index")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    DumpIndex.execute(spark, outpath, query)

    spark.stop()
  }
}
