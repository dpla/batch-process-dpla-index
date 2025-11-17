package dpla.batch_process_dpla_index.processes

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SchemaRepair {
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._

  val jobName = "Avro Schema Repair"

  def main(args: Array[String]): Unit = {
    val inDfPath = args(0)
    val outDfPath = args(1)

    val conf = new SparkConf().setAppName(jobName)
    val spark = SparkSession.builder().master("local[4]").config(conf).getOrCreate()
    val inDf = spark.read.format("avro").load(inDfPath)
    fixSchema(inDf).write.format("avro").save(outDfPath)
  }

  def fixSchema(df: DataFrame): DataFrame = {

    // Helper function to wrap a string column into a struct { value: ... }
    // Handles null safety automatically in Spark SQL expressions
    def wrap(colName: String) = struct(col(colName).as("value"))

    // Helper to transform generic agent arrays (contributor, creator, publisher, etc.)
    // These all require wrapping 'uri' and 'scheme' and 'exactMatch'
    def transformAgent(colName: String) = {
      transform(col(colName), c => struct(
        // Keep existing fields that don't change
        c("name").as("name"),
        c("providedLabel").as("providedLabel"),
        c("note").as("note"),

        // Wrap specific fields from String -> Struct
        struct(c("uri").as("value")).as("uri"),
        struct(c("scheme").as("value")).as("scheme"),

        // Transform Arrays of Strings -> Arrays of Structs
        transform(c("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
        transform(c("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
      ))
    }

    // Helper to transform WebResources (isShownAt, object, preview, etc.)
    // Wraps 'uri' and adds empty 'isReferencedBy' if missing
    def transformWebResource(colName: String) = {
      struct(
        struct(col(s"$colName.uri").as("value")).as("uri"),
        col(s"$colName.fileFormat").as("fileFormat"),
        col(s"$colName.dcRights").as("dcRights"),
        col(s"$colName.edmRights").as("edmRights"),
        // Add missing field as null/empty to match correct schema
        lit(null).cast("struct<value:string>").as("isReferencedBy")
      )
    }

    df.select(
      // 1. Root Level Wrappers
      wrap("dplaUri").as("dplaUri"),

      // 2. Rebuild SourceResource (Renamed from SourceResource)
      struct(
        col("SourceResource.alternateTitle"),
        col("SourceResource.collection"),
        col("SourceResource.title"),

        // Transform Agents [cite: 5, 108]
        transformAgent("SourceResource.contributor").as("contributor"),
        transformAgent("SourceResource.creator").as("creator"),

        col("SourceResource.date"),
        col("SourceResource.description"),
        col("SourceResource.extent"),
        col("SourceResource.format"),

        // Fix Genre (wrap scheme) [cite: 24, 132]
        transform(col("SourceResource.genre"), g => struct(
          g("concept"),
          g("providedLabel"),
          g("note"),
          struct(g("scheme").as("value")).as("scheme"),
          transform(g("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
          transform(g("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
        )).as("genre"),

        col("SourceResource.identifier"),

        // Fix Language (wrap scheme) [cite: 30, 139]
        transform(col("SourceResource.language"), l => struct(
          l("concept"),
          l("providedLabel"),
          l("note"),
          struct(l("scheme").as("value")).as("scheme"),
          transform(l("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
          transform(l("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
        )).as("language"),

        // 3. Flatten Place Hierarchy
        transform(col("SourceResource.place"), p => struct(
          p("name"),
          p("city"),
          // Assuming original 'county' and 'state' were structs holding the nested data
          p("county").cast("string").as("county"),
          p("county.region").as("region"), // Extract nested region
          p("state").cast("string").as("state"),
          p("state.country").as("country"), // Extract nested country
          p("coordinates"),
          // Add exactMatch arrays if missing in source but present in target
          lit(null).cast("array<struct<value:string>>").as("exactMatch")
        )).as("place"),

        transformAgent("SourceResource.publisher").as("publisher"),
        col("SourceResource.relation"),
        col("SourceResource.replacedBy"),
        col("SourceResource.replaces"),
        col("SourceResource.rights"),

        // Fix RightsHolder (wrap uri/scheme) [cite: 49, 163]
        transform(col("SourceResource.rightsHolder"), r => struct(
          r("name"),
          r("note"),
          struct(r("uri").as("value")).as("uri"),
          struct(r("scheme").as("value")).as("scheme"),
          transform(r("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
          transform(r("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
        )).as("rightsHolder"),

        // Fix Subject (wrap scheme) [cite: 56, 172]
        transform(col("SourceResource.subject"), s => struct(
          s("concept"),
          s("providedLabel"),
          s("note"),
          struct(s("scheme").as("value")).as("scheme"),
          transform(s("exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
          transform(s("closeMatch"), x => struct(x.as("value"))).as("closeMatch")
        )).as("subject"),

        col("SourceResource.temporal"),
        col("SourceResource.type")
      ).as("sourceResource"),

      // 4. Transform Providers [cite: 63, 181]
      struct(
        struct(col("dataProvider.uri").as("value")).as("uri"),
        col("dataProvider.name"),
        col("dataProvider.providedLabel"),
        col("dataProvider.note"),
        struct(col("dataProvider.scheme").as("value")).as("scheme"),
        transform(col("dataProvider.exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
        transform(col("dataProvider.closeMatch"), x => struct(x.as("value"))).as("closeMatch")
      ).as("dataProvider"),

      col("originalRecord"),

      // 5. Transform Views/Objects [cite: 76, 200]
      transform(col("hasView"), v => struct(
        struct(v("uri").as("value")).as("uri"),
        v("fileFormat"),
        v("dcRights"),
        v("edmRights"),
        lit(null).cast("struct<value:string>").as("isReferencedBy")
      )).as("hasView"),

      struct(
        struct(col("intermediateProvider.uri").as("value")).as("uri"),
        col("intermediateProvider.name"),
        col("intermediateProvider.providedLabel"),
        col("intermediateProvider.note"),
        struct(col("intermediateProvider.scheme").as("value")).as("scheme"),
        transform(col("intermediateProvider.exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
        transform(col("intermediateProvider.closeMatch"), x => struct(x.as("value"))).as("closeMatch")
      ).as("intermediateProvider"),

      transformWebResource("isShownAt").as("isShownAt"),
      transformWebResource("object").as("object"),
      transformWebResource("preview").as("preview"),

      struct(
        struct(col("provider.uri").as("value")).as("uri"),
        col("provider.name"),
        col("provider.providedLabel"),
        col("provider.note"),
        struct(col("provider.scheme").as("value")).as("scheme"),
        transform(col("provider.exactMatch"), x => struct(x.as("value"))).as("exactMatch"),
        transform(col("provider.closeMatch"), x => struct(x.as("value"))).as("closeMatch")
      ).as("provider"),

      wrap("edmRights").as("edmRights"),
      col("sidecar"),
      col("messages"),
      col("originalId"),

      // 6. Fix tags (Array[String] -> Array[Struct])
      transform(col("tags"), t => struct(t.as("value"))).as("tags"),

      wrap("iiifManifest").as("iiifManifest"),

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
