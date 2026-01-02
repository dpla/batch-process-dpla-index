package dpla.batch_process_dpla_index.processes


import dpla.batch_process_dpla_index.processes.Sitemap.getLatestMasterDatasetPathsForType
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
/*

A utility to dump the schemas of all the latest enriched data to see if there's divergence.

 */
object SchemaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Schema Test")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val paths = getLatestMasterDatasetPathsForType("dpla-master-dataset", "enrichment")

    for {
      (provider, path) <- paths
    } {
      val data = spark.read.format("avro").load(path)
      val schema = data.schema
      Files.write(Path.of(provider.replaceAll("/", "") + ".schema"), schema.sql.getBytes(StandardCharsets.UTF_8))
    }
  }
}
