package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark._

import scala.util.parsing.json._

object JsonlDump extends S3FileWriter with LocalFileWriter with ManifestWriter {

  def execute(spark: SparkSession, outpath: String, query: String): String = {

    // TODO: Changeme
    val maxRows: Int = 40

    val s3write: Boolean = outpath.startsWith("s3")

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val year: String = dateTime.format(DateTimeFormatter.ofPattern("yyyy"))
    val month: String = dateTime.format(DateTimeFormatter.ofPattern("MM"))
    val outDirBase: String = outpath.stripSuffix("/") + "/" + year + "/" + month

    val configs = Map(
      "spark.es.nodes" -> "search-prod1-es6.internal.dp.la",
      "spark.es.mapping.date.rich" -> "false",
      "spark.es.resource" -> "dpla_alias/item",
      "spark.es.query" -> query
    )

    val jsonRdd: RDD[(String, String)] = spark.sqlContext.sparkContext.esJsonRDD(configs)

    val docStrings: RDD[String] = jsonRdd.map{ case(_, doc) => doc }

    docStrings.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val count: Long = docStrings.count

    val numPartitions: Int = (count / maxRows.toFloat).ceil.toInt

    val outDir = outDirBase + "/all.jsonl"

    docStrings.repartition(numPartitions).saveAsTextFile(outDir, classOf[GzipCodec])

    val opts: Map[String, String] = Map(
      "Record count" -> count.toString,
      "Max records per file" -> maxRows.toString,
      "Data source" -> "DPLA ElasticSearch Index")

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)

    // Get a list of distinct provider names
//    val providers: List[String] = docStrings.flatMap(d =>
//      JSON.parseFull(d).flatMap(
//        _.asInstanceOf[Map[String, Any]].get("provider").flatMap(
//          _.asInstanceOf[Map[String,Any]].get("name").map(
//            _.asInstanceOf[String]
//          )
//        )
//      )
//    ).distinct.collect.toList

    // return output path
    outpath
  }


}
