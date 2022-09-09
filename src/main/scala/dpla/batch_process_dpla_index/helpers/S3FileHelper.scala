package dpla.batch_process_dpla_index.helpers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util
import java.util.zip.GZIPOutputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model._

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

trait S3FileHelper {
  lazy val s3client: AmazonS3Client = new AmazonS3Client

  def getBucket(path: String): String = path.split("://")(1).split("/")(0)
  def getKey(path: String): String = path.split("://")(1).split("/").drop(1).mkString("/")

  def deleteS3Path(path: String): Unit = {
    val bucket = getBucket(path)
    val key = getKey(path)

    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucket).withPrefix(key)
    val listObjectsResponse = s3client.listObjects(listObjectsRequest)
    val keys = getS3Keys(listObjectsResponse)

    if (keys.isEmpty)
      return

    deleteS3Keys(bucket, keys)
  }

  def deleteS3Keys(bucket: String, keys: Seq[String]): Unit = {
    val groupedKeys = keys.grouped(1000)
    while(groupedKeys.hasNext) {
      val keyVersions = new util.LinkedList[DeleteObjectsRequest.KeyVersion]
      val group = groupedKeys.next()
      group.map(key => keyVersions.add(new KeyVersion(key)))
      val deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keyVersions)
      s3client.deleteObjects(deleteObjectsRequest)
    }
  }

  def s3ObjectExists(path: String): Boolean = {
    val bucket = getBucket(path)
    val key = getKey(path)

    val req = new ListObjectsRequest().withBucketName(bucket).withPrefix(key)
    val rsp = s3client.listObjects(req)
    rsp.getObjectSummaries.size() > 0
  }

  def writeS3(outpath: String, filename: String, text: String): String = {

    // bucket should have neither protocol nor trailing slash
    val bucket = getBucket(outpath)
    val key = s"${getKey(outpath)}/$filename"

    val in = new ByteArrayInputStream(text.getBytes("utf-8"))
    s3client.putObject(new PutObjectRequest(bucket, key, in, new ObjectMetadata))

    // Return filepath
    s"$bucket/$key"
  }

  def writeS3Gzip(outpath: String, filename: String, text: String): String = {

    val bucket = getBucket(outpath)
    val key = getKey(outpath) + filename

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
