package dpla.batch_process_dpla_index.processes

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.xml.Utility

object Sitemap extends S3FileHelper with LocalFileWriter with ManifestWriter {

  def execute(spark: SparkSession, inPath: String, outPath: String, sitemapUrlPrefix: String): String = {
    import spark.implicits._
    val s3write = outPath.startsWith("s3")

    val maxRows = 50000 // max number of urls in a sitemap subfile

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val isoTimestamp = dateTime.format(DateTimeFormatter.ISO_INSTANT)
    val dirTimestamp = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val parquetPath = PathHelper.parquetPath(inPath)
    val docs = spark.read.parquet(parquetPath)
    val ids = docs.select("doc.id").alias("id").as[String]
    val idCount = ids.count
    val partitionCount = Math.round(Math.ceil(idCount / maxRows)).toInt
    val subFiles = ids.repartition(partitionCount, ids.col("id"))
    val subFileNames = subFiles.rdd.mapPartitionsWithIndex[String]((index, partition) => {
      val subfileBase = dirTimestamp + "/all_item_urls_" + index + ".xml"
      val subfileName = if (s3write) subfileBase + ".gz" else subfileBase
      val subfile = buildSubfile(isoTimestamp, partition)
      if (s3write)
        writeS3Gzip(outPath, subfileName, subfile)
      else
        writeLocal(outPath, subfileName, subfile)
      Seq(subfileName).iterator
    }).collect

    val siteMap = buildIndex(sitemapUrlPrefix, subFileNames, isoTimestamp)
    if (s3write) writeS3(outPath, "all_item_urls.xml", siteMap)
    else writeLocal(outPath, "all_item_urls.xml", siteMap)

    // Manifest

    val opts = Map(
      "Source" -> parquetPath,
      "Subfile directory" -> dirTimestamp,
      "Sitemap URL prefix" -> sitemapUrlPrefix,
      "Total URL count" -> idCount.toString,
      "Max URLs per subfile" -> maxRows.toString)

    val manifest = buildManifest(opts)

    if (s3write) writeS3(outPath, "_MANIFEST", manifest)
    else writeLocal(outPath, "_MANIFEST", manifest)

    // Return output path
    outPath
  }

  def buildSubfile(timestamp: String, ids: Iterator[String]): String = {
    val urls = ids.map(id =>
      <url>
        <loc>
          {"https://dp.la/item/" + id}
        </loc>
        <lastmod>
          {timestamp}
        </lastmod>
        <changefreq>monthly</changefreq>
      </url>
    )

    val xml =
      <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        {urls}
      </urlset>

    Utility.trim(xml).buildString(true)
  }

  def buildIndex(baseUrl: String, subfiles: Seq[String], timestamp: String): String = {

    val sitemapElements = subfiles.map(subfile =>
      <sitemap>
        <loc>
          {baseUrl.stripSuffix("/") + "/" + subfile}
        </loc>
        <lastmod>
          {timestamp}
        </lastmod>
      </sitemap>
    )

    val xml =
      <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        {sitemapElements}
      </sitemapindex>

    Utility.trim(xml).buildString(true)
  }

  def main(args: Array[String]): Unit = {
    val inPath = args(0)
    val outPath = args(1)
    val sitemapUrlPrefix = args(2)
    val conf = new SparkConf().setAppName("Batch process DPLA index: Sitemap")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    Sitemap.execute(spark, inPath, outPath, sitemapUrlPrefix)
    spark.stop()
  }
}
