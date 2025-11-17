package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.S3FileHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object ThumbnailPurge extends S3FileHelper {

  def execute(spark: SparkSession, input: String): String = {

    val df = spark.read.format("avro").load(input)

    val thumbnailBucket = "dpla-thumbnails"
    df.createOrReplaceTempView("df")

    val thumbnailKeys = spark.sql("SELECT df.dplaUri from df")
      .withColumn("dplaId", regexp_replace(col("dplaUri"), "http://dp.la/api/items/", ""))
      .select("dplaId")
      .collect()
      .map(row => {
        thumbnailPrefix(row.getString(0))
      })


    deleteS3Keys(thumbnailBucket, thumbnailKeys)

    s"Deleted ${thumbnailKeys.length} thumbnails"
  }

  def thumbnailPrefix(id: String): String = {
    s"${id(0)}/${id(1)}/${id(2)}/${id(3)}/$id.jpg"
  }

  def take(col: Column, n: Int): Column = {
    assert(n >= 0)
    substring(col, n - 1, n)
  }


  def main(args: Array[String]): Unit = {
    val inpath = args(0)

    val conf = new SparkConf()
      .setAppName("Batch process: Purge Thumbnails")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    ThumbnailPurge.execute(spark, inpath)

    spark.stop()
  }

}
