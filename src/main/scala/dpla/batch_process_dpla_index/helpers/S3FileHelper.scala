package dpla.batch_process_dpla_index.helpers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.GZIPOutputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectListing, ObjectMetadata, PutObjectRequest}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

trait S3FileHelper {
  lazy val s3client: AmazonS3Client = new AmazonS3Client

  def writeS3(outpath: String, key: String, text: String): String = {

    // bucket should have neither protocol nor trailing slash
    val bucket = outpath.split("://")(1).stripSuffix("/")

    val in = new ByteArrayInputStream(text.getBytes("utf-8"))
    val s3client: AmazonS3Client = new AmazonS3Client
    s3client.putObject(new PutObjectRequest(bucket, key, in, new ObjectMetadata))

    // Return filepath
    s"$bucket/$key"
  }

  def writeS3Gzip(outpath: String, key: String, text: String): String = {

    val bucket = outpath.split("://")(1).stripSuffix("/")

    // compress
    val outStream = new ByteArrayOutputStream
    val zipOutStream = new GZIPOutputStream(outStream)
    zipOutStream.write(text.getBytes("utf-8"))
    zipOutStream.close()
    val compressedBytes = outStream.toByteArray

    val inStream = new ByteArrayInputStream(compressedBytes)

    val objectMetadata = new ObjectMetadata()
    objectMetadata.setContentEncoding("gzip")

    s3client.putObject(new PutObjectRequest(bucket, key, inStream, objectMetadata))

    // Return filepath
    s"$bucket/$key"
  }

  @tailrec
  final def getS3Keys(objects: ObjectListing, files: ListBuffer[String] = new ListBuffer[String]): ListBuffer[String] = {
    files ++= objects.getObjectSummaries.toSeq.map(x => x.getKey)
    if (!objects.isTruncated) files
    else getS3Keys(s3client.listNextBatchOfObjects(objects), files)
  }
}