package dpla.batch_process_dpla_index.processes

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object ParquetDump extends LocalFileWriter with S3FileHelper with ManifestWriter {

  def execute(spark: SparkSession, outpath: String, query: String): String = {

    val s3write: Boolean = outpath.startsWith("s3")

    val outDirBase: String = outpath.stripSuffix("/") + PathHelper.outDir + "/all.parquet/"

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)

    val dump: DataFrame = spark.read.format("dpla.datasource").option("query", query)
      .load.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val count: Long = dump.count

    dump.write.parquet(outDirBase)

    val opts: Map[String, String] = Map(
      "Record count" -> count.toString,
      "Data source" -> "DPLA ElasticSearch Index")

    val manifest: String = buildManifest(opts)

    if (s3write) writeS3(outDirBase, "_MANIFEST", manifest)
    else writeLocal(outDirBase, "_MANIFEST", manifest)

    // return output path
    outDirBase
  }
}
