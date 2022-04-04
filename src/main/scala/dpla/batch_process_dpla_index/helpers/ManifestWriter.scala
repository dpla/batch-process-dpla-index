package dpla.batch_process_dpla_index.helpers

import scala.collection.immutable.SortedMap

trait ManifestWriter {

  def buildManifest(opts: Map[String, String]): String = {

    // Sort by key for legibility
    val sorted = SortedMap[String, String]() ++
        opts + ("Start date/time of file generation" -> PathHelper.timestamp)

    sorted.map { case (k, v) => s"$k: $v" }.mkString("\n")
  }
}
