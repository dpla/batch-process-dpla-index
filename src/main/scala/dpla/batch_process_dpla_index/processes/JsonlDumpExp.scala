package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark._
import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

import scala.util.matching.Regex

object JsonlDumpExp extends S3FileWriter with LocalFileWriter with ManifestWriter {

  // 5 mil is too high
  //  val maxRows: Int = 1000000

  def execute(spark: SparkSession, outpath: String, query: String): String = {

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val year: String = dateTime.format(DateTimeFormatter.ofPattern("yyyy"))
    val month: String = dateTime.format(DateTimeFormatter.ofPattern("MM"))
    val outDirBase: String = outpath.stripSuffix("/") + "/" + year + "/" + month

    val bucket = "dpla-master-dataset"

    val allFiles = getS3Keys(s3client.listObjects(bucket)).toList

    val paths = for {
      file <- allFiles
      sections = file.split("/")
      if sections.length > 3
      if sections(1) == "jsonl"
      if sections(2).endsWith(".jsonl")
    } yield (sections(0), sections(2))

    val directories = for {
      group <- paths.groupBy(x => x._1)
      last = group._2.max
    } yield (group._1, "s3a://" + bucket + "/" + group._1 + "/jsonl/" + last._2 + "/")

    import spark.implicits._

    val recordSources: List[(String, RDD[String])] = directories.map(x => {
      val directory = x._2
      val provider = x._1
      val records: DataFrame = spark.read.text(directory).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val recordSource = records.map(
        row => {
          val j = JsonMethods.parse(row.getString(0))
          val source = j \ "_source"
          // There are fields in legacy data files that we either don't need in
          // Ingestion 3, or that are forbidden by Elasticsearch 6:
          val cleanSource = source.removeField {
            case ("_id", _) => true
            case ("_rev", _) => true
            case ("ingestionSequence", _) => true
            case _ => false
          }
          JsonMethods.compact(cleanSource) // "compact" String rendering
        }
      ).rdd

      (provider, recordSource)
    }).toList

    recordSources.foreach(x => {
      val provider = x._1
      val data = x._2
      val outDir = s"$outDirBase/$provider.jsonl"
      export(data, outDir, dateTime)
    })

    outpath
  }

  def export(data: RDD[String], outDir: String, dateTime: ZonedDateTime): Unit = {

    val s3write: Boolean = outDir.startsWith("s3")

    val count: Long = data.count

    //    val numPartitions: Int = (count / maxRows.toFloat).ceil.toInt

    // use repartition, coalesce is too slow
//    data
      //      .repartition(numPartitions)
//      .saveAsTextFile(outDir, classOf[GzipCodec])

    data.saveAsTextFile(outDir, classOf[GzipCodec])

    val opts: Map[String, String] = Map(
      "Record count" -> count.toString,
      //      "Max records per file" -> maxRows.toString,
      "Data source" -> "DPLA ElasticSearch Index")

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }



//    val configs = Map(
//      "spark.es.nodes" -> "search-prod1-es6.internal.dp.la",
//      "spark.es.mapping.date.rich" -> "false",
//      "spark.es.resource" -> "dpla_alias/item",
//      "spark.es.query" -> query
//    )
//
//    val jsonRdd: RDD[(String, String)] = spark.sqlContext.sparkContext.esJsonRDD(configs)
//
//    jsonRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//    jsonRdd.count
//
//    // Use string pattern matching to get provider names b/c parsing JSON is much too expensive.
//    val docs: RDD[(String, String)] = jsonRdd.map { x =>
//      val doc = x._2
//      val j = JsonMethods.parse(doc)
//      val provider = compact(j \ "provider" \ "name")
//      (provider, doc)
//    }
//
//    // Pull docs into memory the first time its evaluated.
////    docs.persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//    docs.count
//
//    outpath
//  }
}
