package dpla.batch_process_dpla_index.processes


import java.util

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model._
import com.amazonaws.{ClientConfiguration, Protocol}
import dpla.batch_process_dpla_index.helpers.S3FileHelper

import scala.collection.JavaConverters._

object TestMain extends App with S3FileHelper{

  val configuration = new ClientConfiguration()

  configuration.setConnectionTimeout(50*1000)

  configuration.setMaxErrorRetry(3)
  configuration.setConnectionTimeout(50*1000)
  configuration.setSocketTimeout(50*1000)
  configuration.setProtocol(Protocol.HTTP)
  configuration.setMaxConnections(10)

  override lazy val s3client = new AmazonS3Client(configuration)

  val path = "s3://dpla-provider-export/2021/07/"
  val deletePath = "s3://dpla-sw/nara/"

  path.split("://")(1).split("/")(0)
  path.split("://")(1).split("/").drop(1).mkString("/")

  s3ObjectExists(path = deletePath)

  deleteS3Path(deletePath)

  override def deleteS3Path(path: String): Unit = {

    val bucket = getBucket(path)
    val key = getKey(path)

    val s3client: AmazonS3Client = new AmazonS3Client

    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucket).withPrefix(key)
    var objectsResponse = s3client.listObjects(listObjectsRequest)
    val keys = getS3Keys(objectsResponse)

    println(s"Found ${keys.size} keys to delete")
    if (keys.isEmpty)
      return

    val groupedKeys = keys.grouped(1000)
    while(groupedKeys.hasNext) {
      val group = groupedKeys.next()
      val keyVersions = new util.LinkedList[DeleteObjectsRequest.KeyVersion]
      group.map(key => keyVersions.add(new KeyVersion(key)))
      val deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keyVersions)
      s3client.deleteObjects(deleteObjectsRequest)

      println(s"Deleted group with ${keyVersions.size()} keys")
    }

  }
}
