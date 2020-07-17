package dpla.batch_process_dpla_index.helpers

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import scala.collection.Map
import scala.util.Try

object ElasticSearchWriter {

  def saveRecords(indexName: String, spark: SparkSession, dataPath: String, esClusterHost: String, esPort: String) = {

    val records: DataFrame = spark.read.parquet(dataPath)

    val configs = Map(
      "es.nodes" -> esClusterHost,
      "es.port" -> esPort,
      "es.mapping.id" -> "id",
      "es.write.operation" -> "upsert",
      "es.index.auto.create" -> "no"
    )

    val status = Try(records.saveToEs(indexName + "/item", configs))
    status match {
      case util.Success(_) => Unit
      case util.Failure(ex) => println(s"ERROR: ${ex.getMessage}")
    }
  }
}
