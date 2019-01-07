package dpla.batch_process_dpla_index.processes

import org.apache.spark.sql.SparkSession

object DumpIndex {

  def execute(spark: SparkSession, outpath: String): String = {

    spark.read.format("dpla.datasource").load.write.parquet(outpath)

    // return output path
    outpath
  }
}
