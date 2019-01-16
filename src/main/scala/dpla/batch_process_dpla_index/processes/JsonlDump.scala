package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._

object JsonlDump extends S3FileWriter with LocalFileWriter with ManifestWriter {

  // 5 mil is too high
  val maxRows: Int = 1000000

  case class ProviderRecords(provider: String, input: String, records: RDD[String], count: Long)

  def execute(spark: SparkSession, outpath: String, query: String): String = {

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val year: String = dateTime.format(DateTimeFormatter.ofPattern("yyyy"))
    val month: String = dateTime.format(DateTimeFormatter.ofPattern("MM"))
    val outDirBase: String = outpath.stripSuffix("/") + "/" + year + "/" + month
    val bucket = "dpla-master-dataset"

    val allFiles = getS3Keys(s3client.listObjects(bucket)).toList

    // Get all paths from S3 bucket
    val paths = for {
      file <- allFiles
      sections = file.split("/")
      if sections.length > 3
      if sections(1) == "jsonl"
      if sections(2).endsWith(".jsonl")
    } yield (sections(0), sections(2))

    // Get the path to the most recent jsonl for each provider
    val directories = for {
      group <- paths.groupBy(x => x._1)
      last = group._2.max
    } yield (group._1, "s3a://" + bucket + "/" + group._1 + "/jsonl/" + last._2 + "/")

    import spark.implicits._

    // Read in jsonl, remove unwanted fields, and persist
    val providerRecords: Iterable[ProviderRecords] = directories.map(x => {
      val input = x._2
      val provider = x._1
      val records: DataFrame = spark.read.text(input)

      val recordSource: RDD[String] = records
        .flatMap(row => cleanJson(row.getString(0)))
        .rdd
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      val count = recordSource.count

      ProviderRecords(provider, input, recordSource, count)
    })

    // Export individual provider dumps
    providerRecords.foreach(x => {
      val outDir = outDirBase + "/" + x.provider + ".jsonl"

      export(x.records, outDir, x.count)

      val manifestOpts: Map[String, String] = Map(
        "Record count" -> x.count.toString,
        //      "Max records per file" -> maxRows.toString,
        "Data source" -> x.input)
      writeManifest(manifestOpts, outDir, dateTime)
    })

    // Export all providers dump
    val allRecords = providerRecords.map(x => x.records).reduce(_.union(_))
    val outDir = s"$outDirBase/all.jsonl"
    val count: Long = providerRecords.map(x => x.count).sum

    export(allRecords, outDir, count)

    val allOpts: Map[String, String] = Map(
      "Total record count" -> count.toString
    )

    val providerOpts: Map[String, String] = providerRecords.map(x => Map(
      x.provider + " date source" -> x.input,
      x.provider + " record count" -> x.count.toString
    )).reduce(_++_)

    writeManifest(allOpts ++ providerOpts, outDir, dateTime)

    outDir
  }

  // Clean up a JSON String by removing unwanted fields.
  // Return only if "_type" == "item"
  def cleanJson(jsonString: String): Option[String] = {
    val j = JsonMethods.parse(jsonString)

    if (j \ "_type" == new JString("item")) {

      val source = j \ "_source"
      // There are fields in legacy data files that we either don't need in
      // Ingestion 3, or that are forbidden by Elasticsearch 6:
      val cleanSource = source.removeField {
        case ("_id", _) => true
        case ("_rev", _) => true
        case ("ingestionSequence", _) => true
        case _ => false
      }

      Some(JsonMethods.compact(cleanSource))

    } else None
  }

  def isItem(jsonString: json4s.JValue): Boolean = jsonString \ "_type" == new JString("item")

  def export(data: RDD[String], outDir: String, count: Long): Unit = {
    val numPartitions: Int = (count / maxRows.toFloat).ceil.toInt
    data.repartition(numPartitions).saveAsTextFile(outDir, classOf[GzipCodec])
  }

  def writeManifest(opts: Map[String, String], outDir: String, dateTime: ZonedDateTime): Unit = {
    val s3write: Boolean = outDir.startsWith("s3")

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }
}
