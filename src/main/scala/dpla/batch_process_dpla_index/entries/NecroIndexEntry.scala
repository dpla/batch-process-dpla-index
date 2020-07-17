package dpla.batch_process_dpla_index.entries

import dpla.batch_process_dpla_index.processes.NecroIndex
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Main entry point for building Necropolis Index.
  *
  * Arguments:
  *
  *   0) inpath         Local or S3 path to necropolis data dump.
  *                     e.g. s3a://dpla-necropolis/2020/07/tombstones.parquet
  *   1) esClusterHost  ES host e.g. "localhost" or "172.30.5.227"
  *   2) esPort         ES port e.g. "9200"
  *   3) indexName      Name ES index that will be created e.g. "mbtest1"
  *   4) shards         Optional: Number of shards for ES index
  *   5) replicas       Optional: Number of replicas for ES index
  *
  * A spark-submit invocation requires the following packages:
  *   org.elasticsearch:elasticsearch-spark-20_2.11:7.3.2
  *   com.amazonaws:aws-java-sdk:1.7.4
  *   org.apache.hadoop:hadoop-aws:2.7.6
  *   com.squareup.okhttp3:okhttp:3.8.0
  *
  *   Double-check build file for correct package versions
  */

object NecroIndexEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)
    val esClusterHost: String = args(1)
    val esPort: String = args(2)
    val indexName: String = args(3)
    val shards: Int = args(4).toInt
    val replicas: Int = args(5).toInt

    val conf: SparkConf = new SparkConf().setAppName("Batch process DPLA index: Build Necropolis Index")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    NecroIndex.execute(spark, inpath, esClusterHost, esPort, indexName, shards, replicas)

    spark.stop()
  }
}
