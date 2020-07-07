package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Necropolis extends S3FileHelper with LocalFileWriter with ManifestWriter {

  // Expects input date in the format "YYYY/MM"
  def execute(spark: SparkSession, newDataPath: String, outpath: String): String = {

    val date: String = newDataPath.stripSuffix("/all.parquet/").split("/").reverse.take(2).reverse.mkString("/")
    val lastDate: String = getLastDate(date)

    val oldDataPath: String = s"s3a://dpla-provider-export/$lastDate/all.parquet/"
    val newTombsPath: String = outpath.stripSuffix("/") + "/" + date + "/tombstones.parquet/"
    val oldTombsPath: String = outpath.stripSuffix("/") + "/" + lastDate + "/tombstones.parquet/"

    val newData: DataFrame = spark.read.parquet(newDataPath).select("doc.id")

    val newTombs: DataFrame = spark.read.parquet(oldDataPath)
      .select(
        col("doc.id"),
        col("doc.provider.name").as("provider"),
        col("doc.dataProvider"),
        col("doc.intermediateProvider"),
        col("doc.isShownAt"),
        col("doc.sourceResource.title").as("title"),
        col("doc.sourceResource.collection.title").as("collection"))
      .join(newData, Seq("id"), "leftanti")

    val oldTombs = spark.read.parquet(oldTombsPath)

    val tombstones = oldTombs.union(newTombs)

    tombstones.write.parquet(newTombsPath)

    val count = spark.read.parquet(newTombsPath).count
    System.out.println("Tombstone count: " + count)

    val opts: Map[String, String] = Map(
      "Ghost records count" -> count.toString,
      "New data source" -> newDataPath,
      "Old data source" -> oldDataPath,
      "Old ghost records source" -> oldTombsPath)

    writeManifest(opts, newTombsPath)

    newTombsPath
  }

  // Expects input date in the format YYYY/MM
  // Returns date in the format YYYY/MM
  def getLastDate(date: String): String = {
    val year = date.split("/")(0)
    val month = date.split("/")(1)
    if (month == "01") (year.toInt - 1).toString + "/12"
    else year + (month.toInt - 1).toString
  }

  def writeManifest(opts: Map[String, String], outDir: String): Unit = {
    val s3write: Boolean = outDir.startsWith("s3")

    val manifest: String = buildManifest(opts)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }
}