package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.MqReports
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
