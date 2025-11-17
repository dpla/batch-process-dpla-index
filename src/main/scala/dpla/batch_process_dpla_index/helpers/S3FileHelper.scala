package dpla.batch_process_dpla_index.helpers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util
import java.util.zip.GZIPOutputStream
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model._

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ListBuffer

trait S3FileHelper {

  private val MAX_ROWS: Int = 2000000

  lazy val s3client: AmazonS3 = AmazonS3Client.builder().build()

  def getLatestMasterDatasetPathsForType(inputBucket: String, dataType: String): Map[String, String] = {
    val listHubFolders = new ListObjectsRequest(inputBucket, "", "", "/", MAX_ROWS)
    val hubsListObjectsResult = s3client.listObjects(listHubFolders)
    val results = for {
      hubFolder <- hubsListObjectsResult.getCommonPrefixes.toSeq
      subFoldersRequest = new ListObjectsRequest(inputBucket, hubFolder + dataType + "/", "", "/", MAX_ROWS)
      subFoldersResult = s3client.listObjects(subFoldersRequest)
      dataTypeFolder <- subFoldersResult.getCommonPrefixes.toSeq.sorted.lastOption
    } yield Tuple2(hubFolder, "s3a://" + inputBucket + "/" + dataTypeFolder)

    results.toMap
  }

  def getBucket(path: String): String = path.split("://")(1).split("/")(0)

  def getKey(path: String): String = path.split("://")(1).split("/").drop(1).mkString("/")

  def deleteS3Path(path: String): Unit = {

    val listObjectsResponse = list(path)
    val keys = getS3Keys(listObjectsResponse)

    if (keys.nonEmpty)
      deleteS3Keys(getBucket(path), keys)
  }

  def deleteS3Keys(bucket: String, keys: Seq[String]): Unit = {
    val groupedKeys = keys.grouped(1000)
    while (groupedKeys.hasNext) {
      val keyVersions = new util.LinkedList[DeleteObjectsRequest.KeyVersion]
      val group = groupedKeys.next()
      group.map(key => keyVersions.add(new KeyVersion(key)))
      val deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keyVersions)
      s3client.deleteObjects(deleteObjectsRequest)
    }
  }


  def s3ObjectExists(path: String): Boolean = {
    val rsp = list(path)
    rsp.getObjectSummaries.size() > 0
  }

  def writeS3(outPath: String, filename: String, text: String): String = {

    // bucket should have neither protocol nor trailing slash
    val bucket = getBucket(outPath)
    val key = s"${getKey(outPath)}/$filename"

    val in = new ByteArrayInputStream(text.getBytes("utf-8"))
    s3client.putObject(new PutObjectRequest(bucket, key, in, new ObjectMetadata))

    // Return filepath
    s"$bucket/$key"
  }

  def writeS3Gzip(outPath: String, filename: String, text: String): String = {

    val bucket = getBucket(outPath)
    val key = s"${getKey(outPath)}/$filename"

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

  private[this] def list(path: String): ObjectListing = {
    val bucket = getBucket(path)
    val key = getKey(path)

    val req = new ListObjectsRequest().withBucketName(bucket).withPrefix(key)
    s3client.listObjects(req)
  }

}
