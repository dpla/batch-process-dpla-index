package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ParquetDump extends LocalFileWriter with S3FileHelper with ManifestWriter {

  private def modifyColumns(df: DataFrame): DataFrame =
    df
      .withColumn("uri", col("dplaUri.value"))
      .withColumn("id", substring_index(col("dplaUri.value"), "http://dp.la/api/items/", -1))
      .drop("dplaUri")
      .drop("originalRecord")
      .withColumn("hasView", transform(col("hasView"), edmWebResource))
      .withColumn("rights", col("edmRights.value"))
      .drop("edmRights")
      .drop("sidecar")
      .drop("messages")
      .withColumn("object", edmWebResource(col("object")))
      .withColumn("preview", edmWebResource(col("preview")))
      .withColumn("iiifManifest", col("iiifManifest").getItem("value"))
      .withColumn("isShownAt", edmWebResource(col("isShownAt")))
      .withColumn("mediaMaster", transform(col("mediaMaster"), edmWebResource))
      .withColumn("provider", edmAgent(col("provider")))
      .withColumn("dataProvider", edmAgent(col("dataProvider")))
      .withColumn("intermediateProvider", edmAgent(col("intermediateProvider")))
      .drop("tags")
      .withColumn("sourceResource", struct(
        col("sourceResource.alternateTitle"),
        transform(col("sourceResource.collection"), dcmiTypeCollection).alias("collection"),
        transform(col("sourceResource.contributor"), edmAgent).alias("contributor"),
        transform(col("sourceResource.creator"), edmAgent).alias("creator"),
        col("sourceResource.date"),
        col("sourceResource.description"),
        col("sourceResource.extent"),
        col("sourceResource.format"),
        col("sourceResource.identifier"),
        transform(col("sourceResource.language"), skosConcept).alias("language"),
        transform(col("sourceResource.place"), dplaPlace).alias("place"),
        transform(col("sourceResource.publisher"), edmAgent).alias("publisher"),
        transform(col("sourceResource.relation"), element => element.getItem("value")).alias("relation"),
        col("sourceResource.replacedBy"),
        col("sourceResource.replaces"),
        col("sourceResource.rights"),
        transform(col("sourceResource.rightsHolder"), edmAgent).alias("rightsHolder"),
        transform(col("sourceResource.subject"), skosConcept).alias("subject"),
        col("sourceResource.temporal"),
        col("sourceResource.title"),
        col("sourceResource.type")
      ))

  private val passthrough: (Column, String) => Column = (col, name) =>
    col.getItem(name).alias(name)

  private val deValue: (Column, String) => Column = (col, name) =>
    col.getItem(name).getItem("value").alias(name)

  private val dplaPlace: Column => Column = col =>
    struct(
      passthrough(col, "name"),
      passthrough(col, "city"),
      passthrough(col, "county"),
      passthrough(col, "state"),
      passthrough(col, "country"),
      passthrough(col, "region"),
      passthrough(col, "coordinates"),
      deValue(col, "exactMatch")
    )

  private val dcmiTypeCollection: Column => Column = col =>
    struct(
      passthrough(col, "title"),
      passthrough(col, "description"),
      edmWebResource(col.getItem("isShownAt")).alias("isShownAt")
    )

  private val skosConcept: Column => Column = col =>
    struct(
      passthrough(col, "concept"),
      passthrough(col, "providedLabel"),
      passthrough(col, "note"),
      deValue(col, "scheme"),
      transform(col.getItem("exactMatch"), element => element.getItem("value")).alias("exactMatch"),
      transform(col.getItem("closeMatch"), element => element.getItem("value")).alias("closeMatch")
    )

  private val edmAgent: Column => Column = col =>
    struct(
      deValue(col, "uri"),
      passthrough(col, "name"),
      passthrough(col, "providedLabel"),
      passthrough(col, "note"),
      deValue(col, "scheme"),
      transform(
        col.getItem("exactMatch"),
        element => element.getItem("value")
      ).alias("exactMatch"),
      transform(
        col.getItem("closeMatch"),
        element => element.getItem("value")
      ).alias("closeMatch")
    )

  private val edmWebResource: Column => Column = col =>
    struct(
      col.getItem("uri").getItem("value").alias("uri"),
      col.getItem("fileFormat").alias("format"),
      col.getItem("dcRights").alias("rights"),
      col.getItem("edmRights").alias("edmRights"),
      col.getItem("isReferencedBy").getItem("value").alias("isReferencedBy")
    )

  private def dump(spark: SparkSession, inPaths: Seq[String], outPath: String): Unit = {
    val df = spark.read.format("avro").load(inPaths: _*)
    val df2 = modifyColumns(df)
    df2.write.parquet(outPath)
  }

  def execute(spark: SparkSession, inBucket: String, outBucket: String): String = {
    val paths = getLatestMasterDatasetPathsForType(inBucket, "enrichment").values.toSeq
    val outPath = PathHelper.parquetPath(outBucket)
    dump(spark, paths, outPath)
    outPath
  }

  private val jobname = "Batch process DPLA index: Parquet Dump"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(jobname)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val inBucket = args(0)
    val outBucket = args(1)
    val result = execute(spark, inBucket, outBucket)
    println("Parquet saved to " + result)
    spark.stop()
  }
}
