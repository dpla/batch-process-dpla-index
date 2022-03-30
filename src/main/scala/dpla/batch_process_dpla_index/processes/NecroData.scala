package dpla.batch_process_dpla_index.processes

import java.time.YearMonth
import java.time.format.DateTimeFormatter

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileHelper}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object NecroData extends S3FileHelper with LocalFileWriter with ManifestWriter {

  // Expects previous date in the format "YYYY/MM".  Default is one month prior to the date in newDataPath.
  def execute(spark: SparkSession, newDataPath: String, outpath: String, previousDate: Option[String]): String = {

    // Parse date from newDataPath in the form YYYY/MM.
    // Expect that this pathname will follow standard naming conventions and thus will end with YYYY/MM/all.parquet
    val date: String = newDataPath.stripSuffix("/").stripSuffix("/all.parquet").split("/").takeRight(2).mkString("/")
    val lastDate: String = previousDate.getOrElse(getLastDate(date))

    val oldDataPath: String = s"s3a://dpla-provider-export/$lastDate/all.parquet/"
    val newTombsPath: String = outpath.stripSuffix("/") + "/" + date + "/tombstones.parquet/"
    val oldTombsPath: String = outpath.stripSuffix("/") + "/" + lastDate + "/tombstones.parquet/"

    // Get only the IDs from the new items.
    val newData: DataFrame = spark.read.parquet(newDataPath).select("doc.id").distinct

    // Get all relevant fields from the old items.
    // Select only those records that appear in the old item dataset but not in the new item dataset.
    // Filter out any records with missing ids.
    val newTombs: DataFrame = spark.read.parquet(oldDataPath)
      .select(
        col("doc.id"),
        col("doc.provider.name").as("provider"),
        col("doc.dataProvider.name").as("dataProvider"),
        col("doc.intermediateProvider"),
        col("doc.isShownAt"),
        col("doc.object"),
        col("doc.sourceResource.title").as("title"),
        col("doc.sourceResource.identifier").as("identifier"),
        flatten(col("doc.sourceResource.collection.title")).as("collection"))
      .distinct
      .join(newData, Seq("id"), "leftanti")
      .where("id is not null")
      .where("id != ''")
      .withColumn("lastActive", lit(lastDate))

    //  Get the old tombstones.
    val oldTombs = spark.read.parquet(oldTombsPath).distinct

    // Get old tombstones whose IDs do not appear in new tombstones.
    // Join the above with new tombstones.
    // In the rare case that an item is taken out of DPLA and put back in multiple times,
    // this ensures that the most recent version of the record is always in the necropolis.
    // Records with duplicate ids may still exist if a single ingest includes duplicate records.
    // These will be cleaned up at index time.
    val tombstones = oldTombs
      .join(newTombs, Seq("id"), "leftanti")
      .union(newTombs)

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
    val year = date.split("/")(0).toInt
    val month = date.split("/")(1).toInt
    YearMonth.of(year, month).minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy/MM"))
  }

  def writeManifest(opts: Map[String, String], outDir: String): Unit = {
    val s3write: Boolean = outDir.startsWith("s3")

    val manifest: String = buildManifest(opts)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)
  }

  // UDF to flatten an array of arrays.
  val toFlat: scala.collection.mutable.WrappedArray[scala.collection.mutable.WrappedArray[String]] =>
    scala.collection.mutable.IndexedSeq[String] = _.flatten
  val flatten: UserDefinedFunction = udf(toFlat)
}
