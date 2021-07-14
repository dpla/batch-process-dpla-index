package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDump extends LocalFileWriter with S3FileHelper with ManifestWriter {

  def execute(spark: SparkSession, outpath: String, query: String): String = {

    val s3write: Boolean = outpath.startsWith("s3")

    val outDirBase: String = PathHelper.parquetPath(outpath)

    if (s3write && s3ObjectExists(outDirBase)) {
      println(s"Deleting existing keys under $outDirBase")
      deleteS3Path(outDirBase) // parquet out
    }

    // Read data from ElasticSearch and save as parquet

    val dump: DataFrame = spark.read.format("dpla.datasource").option("query", query).load

    dump.write.parquet(outDirBase)

    // Read back parquet file that was just written
    // This allows us to avoid persisting ElasticSearch dump in memory

    val readBack: DataFrame = spark.read.parquet(outDirBase)

    val count: Long = readBack.count

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
