package dpla.batch_process_dpla_index.processes

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object ParquetDump extends LocalFileWriter with S3FileWriter with ManifestWriter {

  def execute(spark: SparkSession, outpath: String, query: String): String = {

    val s3write: Boolean = outpath.startsWith("s3")

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val dirTimestamp: String = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    val dump: DataFrame = spark.read.format("dpla.datasource").option("query", query)
      .load.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val count: Long = dump.count

    val outDir: String = outpath + "/" + dirTimestamp + "-DplaIndexDump.parquet"
    dump.write.parquet(outDir)

    val opts: Map[String, String] = Map(
      "Record count" -> count.toString,
      "Data source" -> "DPLA ElasticSearch Index")

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)

    // return output path
    outDir
  }
}
