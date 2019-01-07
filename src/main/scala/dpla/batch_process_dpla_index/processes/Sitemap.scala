package dpla.batch_process_dpla_index.processes

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, S3FileWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Sitemap extends S3FileWriter with LocalFileWriter {

  def execute(spark: SparkSession, inpath: String, outpath: String, sitemapUrlPrefix: String): String = {

    val s3write: Boolean = outpath.startsWith("s3")

    val timestamp = LocalDateTime.now().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
    val maxRows: Int = 50000

    val docs = spark.read.parquet(inpath)

    val ids: RDD[String] = docs.select("doc.id").rdd.map{ row => row.getString(0) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // force evaluation to pull RDD into memory
    val id_count = ids.count

    val subfiles: Iterator[String] = ids.toLocalIterator.grouped(maxRows).zipWithIndex.map { case (ids, seq) => {
      val subfileName = timestamp + "/all_item_urls_" + seq + ".xml"
      val subfile = buildSubfile(timestamp, ids)

      if (s3write) writeS3Gzip(outpath, subfileName, subfile)
      else writeLocal(outpath, subfileName, subfile)
    }}

    val siteMap: String = buildIndex(sitemapUrlPrefix, subfiles, timestamp)

    if (s3write) writeS3(outpath, "sitemap.xml", siteMap)
    else writeLocal(outpath, "sitemap.xml", siteMap)

    // return output path
    outpath
  }

  def buildSubfile(timestamp: String, ids: Iterable[String]): String = {

    val urls = ids.map(
      id => {
        <url>
          <loc>{"https://dp.la/item/" + id}</loc>
          <lastmod>{timestamp}</lastmod>
          <changefreq>monthly</changefreq>
        </url>
      }
    )

    val xml = <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      {urls}
    </urlset>

    xml.buildString(true)
  }

  def buildIndex(baseUrl: String, subfiles: Iterator[String], timestamp: String): String  = {

    val sitemapElements = subfiles.map( subfile => {
      val url: String = baseUrl + subfile
      <sitemap><loc>{url}</loc><lastmod>{timestamp}</lastmod></sitemap>
    })

    val xmlData =
      <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{sitemapElements}</sitemapindex>

    xmlData.toString
  }
}
