package dpla.batch_process_dpla_index.helpers

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.SortedMap

trait ManifestWriter {

  def buildManifest(opts: Map[String, String], dateTime: ZonedDateTime): String = {
    val date: String = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    // Add date/time to given `opts'
    val data: Map[String, String] = opts + ("Start date/time of file generation" -> date)

    // Sort by key for legibility
    val sorted = SortedMap[String, String]() ++ data

    sorted.map{ case(k, v) => s"$k: $v" }.mkString("\n")
  }
}
