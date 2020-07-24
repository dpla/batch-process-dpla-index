package dpla.batch_process_dpla_index.helpers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import scala.collection.Map
import scala.util.Try

object ElasticSearchWriter {

  def saveRecords(index: Index, spark: SparkSession, dataPath: String) = {

    val records: DataFrame = spark.read.parquet(dataPath)

    val configs = Map(
      "es.nodes" -> index.host,
      "es.port" -> index.port,
      "es.mapping.id" -> "id",
      "es.write.operation" -> "upsert",
      "es.index.auto.create" -> "no"
    )

    val status = Try(records.saveToEs(index.indexName + "/item", configs))
    status match {
      case util.Success(_) => Unit
      case util.Failure(ex) => println(s"ERROR: ${ex.getMessage}")
    }
  }
}
