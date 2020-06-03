package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MqReports extends LocalFileWriter with S3FileHelper with ManifestWriter {

  def execute(spark: SparkSession, inpath: String, outpath: String): String = {

    val s3write: Boolean = outpath.startsWith("s3")
    val outDir: String = outpath.stripSuffix("/") + PathHelper.outDir

    val docs: DataFrame = spark.read.parquet(inpath)

    val items = docs.select("doc.*")

    items.createOrReplaceTempView("items")

    val itemdata = spark.sqlContext.sql("""select id,
                                         provider.name as provider,
                                         dataProvider as dataProviders,
                                        case
                                          when size(sourceResource.title) == 0
                                          then 0 else 1 end
                                          as title,
                                        case
                                          when size(sourceResource.description) == 0
                                          then 0 else 1 end
                                          as description,
                                        case
                                          when size(sourceResource.creator) == 0
                                          then 0 else 1 end
                                          as creator,
                                        case
                                          when size(sourceResource.type) == 0
                                          then 0 else 1 end
                                          as type,
                                        case
                                          when size(sourceResource.language.name) == 0
                                          then 0 else 1 end
                                          as language,
                                        case
                                          when size(sourceResource.spatial.name) == 0
                                          then 0 else 1 end
                                          as spatial,
                                        case
                                          when size(sourceResource.subject.name) == 0
                                          then 0 else 1 end
                                          as subject,
                                        case
                                          when size(sourceResource.date.displayDate) == 0
                                          then 0 else 1 end
                                          as date,
                                        case
                                          when rights is null
                                          then 0 else 1 end
                                          as standardizedRights,
                                        case
                                          when rights LIKE '%/NoC-US/%'
                                            or rights LIKE '%/publicdomain/%'
                                            or rights LIKE '%/by/%'
                                            or rights LIKE '%/by-sa/%'
                                          then 1 else 0 end
                                          as openLicense,
                                        case
                                          when size(sourceResource.rights) == 0
                                          then 0 else 1 end
                                          as rights,
                                        case
                                          when size(object) == 0
                                          then 0 else 1 end
                                          as preview,
                                        case
                                          when iiifManifest is null
                                          then 0 else 1 end
                                          as iiifManifest,
                                        case
                                          when size(mediaMaster) == 0
                                          then 0 else 1 end
                                          as mediaMaster,
                                        case
                                          when iiifManifest is null and size(mediaMaster) == 0
                                          then 0 else 1 end
                                          as mediaAccess
                                        from items""")

    val providerScores = itemdata.filter("provider is not null")
      .drop("dataProviders")
      .withColumn("wikimediaReady", expr("case when mediaAccess == 1 and openLicense == 1 then 1 else 0 end"))
      .withColumn("count", lit(1))
      .groupBy("provider")
      .agg(mean("title").alias("title"),
        mean("description").alias("description"),
        mean("creator").alias("creator"),
        mean("type").alias("type"),
        mean("language").alias("language"),
        mean("spatial").alias("spatial"),
        mean("subject").alias("subject"),
        mean("date").alias("date"),
        mean("rights").alias("rights"),
        mean("standardizedRights").alias("standardizedRights"),
        mean("preview").alias("preview"),
        mean("iiifManifest").alias("iiifManifest"),
        mean("mediaMaster").alias("mediaMaster"),
        mean("mediaAccess").alias("mediaAccess"),
        mean("openLicense").alias("openLicense"),
        mean("wikimediaReady").alias("wikimediaReady"),
        sum("count").alias("count"))

    val contributorScores = itemdata.filter("provider is not null")
      .withColumn("dataProvider", explode(col("dataProviders")))
      .filter("dataProvider is not null")
      .withColumn("wikimediaReady", expr("case when mediaAccess == 1 and openLicense == 1 then 1 else 0 end"))
      .withColumn("count", lit(1))
      .groupBy("dataProvider", "provider")
      .agg(mean("title").alias("title"),
        mean("description").alias("description"),
        mean("creator").alias("creator"),
        mean("type").alias("type"),
        mean("language").alias("language"),
        mean("spatial").alias("spatial"),
        mean("subject").alias("subject"),
        mean("date").alias("date"),
        mean("rights").alias("rights"),
        mean("standardizedRights").alias("standardizedRights"),
        mean("preview").alias("preview"),
        mean("iiifManifest").alias("iiifManifest"),
        mean("mediaMaster").alias("mediaMaster"),
        mean("mediaAccess").alias("mediaAccess"),
        mean("openLicense").alias("openLicense"),
        mean("wikimediaReady").alias("wikimediaReady"),
        sum("count").alias("count"))

    providerScores
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(outDir + "/provider.csv")

    contributorScores
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(outDir + "/contributor.csv")

    val opts: Map[String, String] = Map(
      "Source" -> inpath,
      "Provider count" -> providerScores.count.toString,
      "Contributor count" -> contributorScores.count.toString
    )
    val manifest: String = buildManifest(opts)

    if (s3write) writeS3(outDir, "_MANIFEST", manifest)
    else writeLocal(outDir, "_MANIFEST", manifest)

    // return outpath
    outDir
  }
}
