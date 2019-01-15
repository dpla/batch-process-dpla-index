package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods

object JsonlDumpExp extends S3FileWriter with LocalFileWriter with ManifestWriter {

  // 5 mil is too high
  //  val maxRows: Int = 1000000\

  case class ProviderRecords(provider: String, input: String, records: RDD[String], count: Long)

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

    val providerRecords: Iterable[ProviderRecords] = directories.map(x => {
      val input = x._2
      val provider = x._1
      val records: DataFrame = spark.read.text(input)

      val recordSource: RDD[String] = records.map(
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
      ).rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

      val count = recordSource.count

      ProviderRecords(provider, input, recordSource, count)
    })

    // Export individual provider dumps
    providerRecords.foreach(x => {
      val outDir = "outDirBase/" + x.provider + ".jsonl"

      export(x.records, outDir)

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

    export(allRecords, outDir)

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

  def export(data: RDD[String], outDir: String): Unit = {

    val s3write: Boolean = outDir.startsWith("s3")
    data.saveAsTextFile(outDir, classOf[GzipCodec])


    //    val numPartitions: Int = (count / maxRows.toFloat).ceil.toInt

    // use repartition, coalesce is too slow
//    data
      //      .repartition(numPartitions)
//      .saveAsTextFile(outDir, classOf[GzipCodec])

  }

  def writeManifest(opts: Map[String, String], outDir: String, dateTime: ZonedDateTime) = {
    val s3write: Boolean = outDir.startsWith("s3")

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }
}
