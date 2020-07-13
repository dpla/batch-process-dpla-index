package dpla.batch_process_dpla_index.processes

import dpla.batch_process_dpla_index.helpers.{LocalFileWriter, ManifestWriter, PathHelper, S3FileHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object NecroIndex extends ManifestWriter {

  def execute(spark: SparkSession, inpath: String): Unit = {

    val tombstones: DataFrame = spark.read.parquet(inpath)

   
  }
}
