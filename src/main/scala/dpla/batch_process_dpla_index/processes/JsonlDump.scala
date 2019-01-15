package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark._

import scala.util.matching.Regex

object JsonlDump extends S3FileWriter with LocalFileWriter with ManifestWriter {

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

    // Use string pattern matching to get provider names b/c parsing JSON is much too expensive.
//    val docs: RDD[(String, String)] = jsonRdd.flatMap { case(_, doc) =>
//      // match pattern "provider":{"[...]}"
//      val providerSubstring = "\"provider\":\\{[^}]*\\}".r.findFirstIn(doc)
//      // match pattern "name":"[...]"
//      "(\"name\":\")([^\"]*)".r.findFirstMatchIn(providerSubstring.getOrElse("")) match {
//        case Some(m) => Some((m.group(2), doc))
//        case None => None
//      }
//    }

    val docs: RDD[(String, String)] = jsonRdd.flatMap { case (_, doc) => docParser.parse(doc) }

    // Pull docs into memory the first time its evaluated.
    docs.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val allJsonStrings = docs.map(_._2)
    export(allJsonStrings, s"$outDirBase/all.jsonl", dateTime)

    val providers: Array[String] = docs.keys.distinct.collect

    providers.foreach(p => {
      val jsonStrings = docs.filter(_._1 == p).map(_._2)
      val label = p.replace(" ", "_")
      export(jsonStrings, s"$outDirBase/$label.jsonl", dateTime)
    })

    outDirBase
  }

  def outDir(label: String, outDirBase: String): String = outDirBase + "/" + label + ".jsonl"

  def export(data: RDD[String], outDir: String, dateTime: ZonedDateTime): Unit = {

    val s3write: Boolean = outDir.startsWith("s3")

    val count: Long = data.count

//    val numPartitions: Int = (count / maxRows.toFloat).ceil.toInt

    // use repartition, coalesce is too slow
    data
//      .repartition(numPartitions)
      .saveAsTextFile(outDir, classOf[GzipCodec])

    val opts: Map[String, String] = Map(
      "Record count" -> count.toString,
//      "Max records per file" -> maxRows.toString,
      "Data source" -> "DPLA ElasticSearch Index")

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }
}

object docParser {
  val providerRegex: Regex = "\"provider\":\\{[^}]*\\}".r
  val nameRegex: Regex = "(\"name\":\")([^\"]*)".r

  def parse(doc: String): Option[(String, String)] = {
    val provider: Option[String] = providerRegex.findFirstIn(doc)
    nameRegex.findFirstMatchIn(provider.getOrElse("")) match {
      case Some(m) => Some((m.group(2), doc))
      case None => None
    }
  }
}
