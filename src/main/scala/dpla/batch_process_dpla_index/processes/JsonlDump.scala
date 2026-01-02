package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object JsonlDump extends S3FileHelper with LocalFileWriter with ManifestWriter {
  val inputBucket = "dpla-master-dataset"

  private case class ProviderRecords(provider: String, input: String, records: RDD[String], count: Long)

  def execute(spark: SparkSession, inputBucket: String, outputBucket: String): String = {
    val outDirBase = outputBucket.stripSuffix("/") + PathHelper.datePath
    val hubToJsonl = getLatestMasterDatasetPathsForType(inputBucket, "jsonl")

    import spark.implicits._

    // Read in jsonl, remove unwanted fields, and persist
    val providerRecords: Iterable[ProviderRecords] = hubToJsonl.map {
      case (provider: String, input: String) =>
        val records = spark.read.text(input)

        val recordSource: RDD[String] = records
          .map(row => row.getString(0))
          .rdd
          .persist(StorageLevel.DISK_ONLY)

        // Read input into memory
        val count = recordSource.count

        ProviderRecords(provider, input, recordSource, count)
    }

    // Export individual provider dumps
    providerRecords.foreach(x => {
      val outDir = outDirBase + "/" + x.provider.replaceAll("/", "") + ".jsonl"

      deleteExisting(outDir)
      export(x.records, outDir)

      val manifestOpts: Map[String, String] = Map(
        "Record count" -> x.count.toString,
        "Data source" -> x.input)
      writeManifest(manifestOpts, outDir)
    })

    // Export all providers dump
    val allRecords = providerRecords.map(x => x.records).reduce(_.union(_))
    val outDir = s"$outDirBase/all.jsonl"
    val count: Long = providerRecords.map(x => x.count).sum

    deleteExisting(outDir)
    export(allRecords, outDir)

    val allOpts: Map[String, String] = Map(
      "Total record count" -> count.toString,
    )

    val providerOpts: Map[String, String] = providerRecords.map(x => Map(
      x.provider + " date source" -> x.input,
      x.provider + " record count" -> x.count.toString
    )).reduce(_ ++ _)

    writeManifest(allOpts ++ providerOpts, outDir)

    outDir
  }

  private def deleteExisting(outDir: String): Unit =
    if (s3ObjectExists(outDir))
      deleteS3Path(outDir)

  private def export(data: RDD[String], outDir: String): Unit =
    data.saveAsTextFile(outDir, classOf[GzipCodec])

  private def writeManifest(opts: Map[String, String], outDir: String): Unit = {
    val s3write = outDir.startsWith("s3")

    val manifest = buildManifest(opts)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }

  private val jobname = "Batch process DPLA index: JSON-L Dump"

  def main(args: Array[String]): Unit = {
    val inputBucket = args(0)
    val outputBucket = args(1)
    val conf = new SparkConf().setAppName(jobname)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    JsonlDump.execute(spark, inputBucket, outputBucket)
    spark.stop()
  }
}
