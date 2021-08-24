package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.ThumbnailPurge
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Run as
  * 
  * sbt "runMain dpla.batch_process_dpla_index.entries.ThumbnailPurgeEntry ./data/20210706_151435-virginias-MAP4_0.MAPRecord.avro/"
  */
object ThumbnailPurgeEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)

    val conf: SparkConf = new SparkConf()
      .setAppName("Batch process: Purge Thumbnails")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    ThumbnailPurge.execute(spark, inpath)

    spark.stop()
  }
}
