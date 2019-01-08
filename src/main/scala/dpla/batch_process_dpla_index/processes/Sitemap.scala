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

    val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
    val maxRows: Int = 50000

    val docs: DataFrame = spark.read.parquet(inpath)

    val ids: RDD[String] = docs.select("doc.id").rdd.map{ row => row.getString(0) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // force evaluation to pull RDD into memory
    val id_count: Long = ids.count

    val subfiles: Iterator[String] = ids.toLocalIterator.grouped(maxRows).zipWithIndex.map { case (ids, seq) => {

      val timestamp: String = dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
      val subfileBase = timestamp + "/all_item_urls_" + seq + ".xml"
      val subfileName = if (s3write) subfileBase + ".gz" else subfileBase

      val subfile: String = buildSubfile(dateTime, ids)

      if (s3write) writeS3Gzip(outpath, subfileName, subfile)
      else writeLocal(outpath, subfileName, subfile)

      subfileName
    }}

    val siteMap: String = buildIndex(sitemapUrlPrefix, subfiles, dateTime)

    if (s3write) writeS3(outpath, "all_item_urls.xml", siteMap)
    else writeLocal(outpath, "all_item_urls.xml", siteMap)

    val opts: Map[String, String] =
      Map("Source" -> inpath, "Sitemap URL prefix" -> sitemapUrlPrefix, "URL count" -> id_count.toString)
    val manifest: String = buildManifest(opts, dateTime)

    if (s3write) writeS3(outpath, "_MANIFEST", manifest)
    else writeLocal(outpath, "_MANIFEST", manifest)

    // return output path
    outpath
  }

  def buildSubfile(dateTime: ZonedDateTime, ids: Iterable[String]): String = {

    val timestamp: String = dateTime.format(DateTimeFormatter.ISO_INSTANT)

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

  def buildIndex(baseUrl: String, subfiles: Iterator[String], dateTime: ZonedDateTime): String  = {

    val timestamp: String = dateTime.format(DateTimeFormatter.ISO_INSTANT)

    val sitemapElements: Iterator[Elem] = subfiles.map( subfile => {
      val url: String = baseUrl.stripSuffix("/") + "/" + subfile
      <sitemap><loc>{url}</loc><lastmod>{timestamp}</lastmod></sitemap>
    })

    val xmlData: Elem =
      <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{sitemapElements}</sitemapindex>

    xmlData.toString
  }
}
