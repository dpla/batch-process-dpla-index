package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark._
import org.json4s.jackson.JsonMethods
import org.json4s.JsonDSL._
import org.json4s.JValue

import scala.util.matching.Regex

object JsonlDumpExp extends S3FileWriter with LocalFileWriter with ManifestWriter {

  // 5 mil is too high
  //  val maxRows: Int = 1000000

  def execute(spark: SparkSession, outpath: String, query: String): String = {

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

    jsonRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Use string pattern matching to get provider names b/c parsing JSON is much too expensive.
    val docs: RDD[(String, String)] = jsonRdd.map { x =>
      val doc = x._2
      // match pattern "provider":{"[...]}"
      val providerSubstring = "\"provider\":\\{[^}]*\\}".r.findFirstIn(doc)
      // match pattern "name":"[...]"
      "(\"name\":\")([^\"]*)".r.findFirstMatchIn(providerSubstring.getOrElse("")) match {
        case Some(m) => (m.group(2), doc)
        case None => ("MISSING", doc)
      }
    }

    // Pull docs into memory the first time its evaluated.
//    docs.persist(StorageLevel.MEMORY_AND_DISK_SER)

    docs.count

    outpath
  }
}
