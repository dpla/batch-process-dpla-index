package dpla.batch_process_dpla_index.processes

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SchemaRepair {

  private val jobName = "Avro Schema Repair"

  /*

  NB: ESDN's data did not have the iiifManifest nor mediaMaster fields,
  so I had to manually add them with a spark-shell command like:

   res0
    .withColumn("iiifManifest", lit(null).cast("struct<value:string>"))
    .withColumn("mediaMaster", array().cast("array<struct<uri:struct<value:string>,fileFormat:array<string>,dcRights:array<string>,edmRights:string,isReferencedBy:struct<value:string>>>"))
    .write.format("avro").save("fixed2")

   ... before running this class on it. I feel like a wizard now.

   */

  def main(args: Array[String]): Unit = {
    val inDfPath = args(0)
    val outDfPath = args(1)
    val conf = new SparkConf().setAppName(jobName)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val inDf = spark.read.format("avro").load(inDfPath)
    val outDf = fixSchema(inDf)
    outDf.write.format("avro").save(outDfPath)
    spark.stop()
  }

  private def toValueField(colName: String): Column =
    struct(col(colName).as("value")).as(colName)

  private def transformAgent(col: Column): Column = {
    struct(
      struct(col.getField("uri").as("value")).as("uri"),
      col.getField("name").as("name"),
      col.getField("providedLabel").as("providedLabel"),
      col.getField("note").as("note"),
      struct(col.getField("scheme").as("value")).as("scheme"),
      transform(col.getField("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
      transform(col.getField("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
    )
  }

  private def transformArray(colName: String, function: Column => Column): Column =
    transform(col(colName), c => function(c))

  private def transformWebResource(col: Column): Column =
    struct(
      struct(col.getField("uri").as("value")).as("uri"),
      col.getField("fileFormat").as("fileFormat"),
      col.getField("dcRights").as("dcRights"),
      col.getField("edmRights").as("edmRights"),
      lit(null).cast("struct<value:string>").as("isReferencedBy"),
    )

  private def transformDcmiTypeCollection(origCol: Column): Column =
    struct(
      origCol.getField("title"),
      origCol.getField("description"),
      lit(null)
        .cast("struct<uri:struct<value:string>,fileFormat:array<string>,dcRights:array<string>,edmRights:string,isReferencedBy:struct<value:string>>")
        .as("isShownAt"),
    )

  private def transformSkosConcept(origCol: Column): Column =
    struct(
      origCol.getField("concept"),
      origCol.getField("providedLabel"),
      origCol.getField("note"),
      struct(origCol.getField("scheme").as("value")).as("scheme"),
      transform(origCol.getField("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
      transform(origCol.getField("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
    )

  private def transformDplaPlace(): Column = transform(
    col("SourceResource.place"),
    place => place.withField("exactMatch", array(lit(null).cast("struct<value:string>")))
  )

  private def fixSchema(df: DataFrame): DataFrame = {

    df.select(
      toValueField("dplaUri"),
      struct(
        col("SourceResource.alternateTitle"),
        transformArray("SourceResource.collection", transformDcmiTypeCollection).as("collection"),
        transformArray("SourceResource.contributor", transformAgent).as("contributor"),
        transformArray("SourceResource.creator", transformAgent).as("creator"),
        col("SourceResource.date"),
        col("SourceResource.description"),
        col("SourceResource.extent"),
        col("SourceResource.format"),
        transformArray("SourceResource.genre", transformSkosConcept).as("genre"),
        col("SourceResource.identifier"),
        transformArray("SourceResource.language", transformSkosConcept).as("language"),
        transformDplaPlace().as("place"),
        transformArray("SourceResource.publisher", transformAgent).as("publisher"),
        col("SourceResource.relation"),
        col("SourceResource.replacedBy"),
        col("SourceResource.replaces"),
        col("SourceResource.rights"),
        transformArray("SourceResource.rightsHolder", transformAgent).as("rightsHolder"),
        transformArray("SourceResource.subject", transformSkosConcept).as("subject"),
        col("SourceResource.temporal"),
        col("SourceResource.title"),
        col("SourceResource.type")
      ).as("sourceResource"),
      transformAgent(col("dataProvider")).as("dataProvider"),
      col("originalRecord"),
      transformArray("hasView", transformWebResource).as("hasView"),
      transformAgent(col("intermediateProvider")).as("intermediateProvider"),
      transformWebResource(col("isShownAt")).as("isShownAt"),
      transformWebResource(col("object")).as("object"),
      transformWebResource(col("preview")).as("preview"),
      transformAgent(col("provider")).as("provider"),
      toValueField("edmRights"),
      col("sidecar"),
      col("messages"),
      col("originalId"),
      transform(col("tags"), t => struct(t.as("value"))).as("tags"),
      toValueField("iiifManifest").as("iiifManifest"),
      transform(col("mediaMaster"), m => struct(
        struct(m("uri").as("value")).as("uri"),
        m("fileFormat"),
        m("dcRights"),
        m("edmRights"),
        lit(null).cast("struct<value:string>").as("isReferencedBy")
      )).as("mediaMaster")
    )
  }
}
