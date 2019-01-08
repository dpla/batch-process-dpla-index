package dpla.batch_process_dpla_index.processes

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, S3FileWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.xml.Elem

object Sitemap extends S3FileWriter with LocalFileWriter with ManifestWriter {

  def execute(spark: SparkSession, inpath: String, outpath: String, sitemapUrlPrefix: String): String = {

    val s3write: Boolean = outpath.startsWith("s3")

    val maxRows: Int = 50000

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val isoTimestamp = dateTime.format(DateTimeFormatter.ISO_INSTANT)
    val dirTimestamp = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    val docs: DataFrame = spark.read.parquet(inpath)

    val ids: RDD[String] = docs.select("doc.id").rdd.map{ row => row.getString(0) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Force evaluation to pull RDD into memory
    val id_count: Long = ids.count

    // Subfiles

    val subfiles: Iterator[String] = ids.toLocalIterator.grouped(maxRows).zipWithIndex.map { case (ids, seq) => {

      val subfileBase = dirTimestamp + "/all_item_urls_" + seq + ".xml"
      val subfileName = if (s3write) subfileBase + ".gz" else subfileBase

      val subfile: String = buildSubfile(isoTimestamp, ids)

      if (s3write) writeS3Gzip(outpath, subfileName, subfile)
      else writeLocal(outpath, subfileName, subfile)

      subfileName
    }}

    // Sitemap index

    val siteMap: String = buildIndex(sitemapUrlPrefix, subfiles, isoTimestamp)

    if (s3write) writeS3(outpath, "all_item_urls.xml", siteMap)
    else writeLocal(outpath, "all_item_urls.xml", siteMap)

    // Manifest

    val opts: Map[String, String] = Map(
      "Source" -> inpath,
      "Subfile directory" -> dirTimestamp,
      "Sitemap URL prefix" -> sitemapUrlPrefix,
      "Total URL count" -> id_count.toString,
      "URLs per subfile" -> maxRows.toString)

    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outpath, "_MANIFEST", manifest)
    else writeLocal(outpath, "_MANIFEST", manifest)

    // Return output path
    outpath
  }

  def buildSubfile(timestamp: String, ids: Iterable[String]): String = {

    val urls: Iterable[Elem] = ids.map(
      id => {
        <url>
          <loc>{"https://dp.la/item/" + id}</loc>
          <lastmod>{timestamp}</lastmod>
          <changefreq>monthly</changefreq>
        </url>
      }
    )

    val xml: Elem = <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      {urls}
    </urlset>

    xml.buildString(true)
  }

  def buildIndex(baseUrl: String, subfiles: Iterator[String], timestamp: String): String  = {

    val sitemapElements: Iterator[Elem] = subfiles.map( subfile => {
      val url: String = baseUrl.stripSuffix("/") + "/" + subfile
      <sitemap><loc>{url}</loc><lastmod>{timestamp}</lastmod></sitemap>
    })

    val xmlData: Elem =
      <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{sitemapElements}</sitemapindex>

    xmlData.toString
  }
}
