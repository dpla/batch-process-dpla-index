package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object JsonlDump extends S3FileHelper with LocalFileWriter with ManifestWriter {

  // Maximum rows per record (approximate).
  // Performance slows at 5 mil records, errors occur at 10 mil.
  val maxRows: Int = 2000000

  val inputBucket = "dpla-master-dataset"

  case class ProviderRecords(provider: String, input: String, records: RDD[String], count: Long)

  def execute(spark: SparkSession, outpath: String): String = {

    val outDirBase: String = outpath.stripSuffix("/") + PathHelper.datePath

    val allFiles = getS3Keys(s3client.listObjects(inputBucket)).toList

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
    } yield (group._1, "s3a://" + inputBucket + "/" + group._1 + "/jsonl/" + last._2 + "/")

    import spark.implicits._

    // Read in jsonl, remove unwanted fields, and persist
    val providerRecords: Iterable[ProviderRecords] = directories.map(x => {
      val input = x._2
      val provider = x._1
      val records: DataFrame = spark.read.text(input)

      val recordSource: RDD[String] = records
        .map(row => row.getString(0))
        .rdd
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // Read input into memory
      val count = recordSource.count

      ProviderRecords(provider, input, recordSource, count)
    })

    // Export individual provider dumps
    providerRecords.foreach(x => {
      val outDir = outDirBase + "/" + x.provider + ".jsonl"

      deleteExisting(outDir)
      export(x.records, outDir, x.count)

      val manifestOpts: Map[String, String] = Map(
        "Record count" -> x.count.toString,
        "Max records per file (approximate)" -> maxRows.toString,
        "Data source" -> x.input)
      writeManifest(manifestOpts, outDir)
    })

    // Export all providers dump
    val allRecords = providerRecords.map(x => x.records).reduce(_.union(_))
    val outDir = s"$outDirBase/all.jsonl"
    val count: Long = providerRecords.map(x => x.count).sum

    deleteExisting(outDir)
    export(allRecords, outDir, count)

    val allOpts: Map[String, String] = Map(
      "Total record count" -> count.toString,
      "Max records per file (approximate)" -> maxRows.toString
    )

    val providerOpts: Map[String, String] = providerRecords.map(x => Map(
      x.provider + " date source" -> x.input,
      x.provider + " record count" -> x.count.toString
    )).reduce(_++_)

    writeManifest(allOpts ++ providerOpts, outDir)

    outDir
  }

  def deleteExisting(outDir: String): Unit = {
    if(s3ObjectExists(outDir))
      deleteS3Path(outDir)
  }

  def export(data: RDD[String], outDir: String, count: Long): Unit = {
    val numPartitions: Int = (count / maxRows.toFloat).ceil.toInt
    data.repartition(numPartitions).saveAsTextFile(outDir, classOf[GzipCodec])
  }

  def writeManifest(opts: Map[String, String], outDir: String): Unit = {
    val s3write: Boolean = outDir.startsWith("s3")

    val manifest: String = buildManifest(opts)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }
}
