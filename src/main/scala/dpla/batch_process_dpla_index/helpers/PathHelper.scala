package dpla.batch_process_dpla_index.helpers

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

object PathHelper {
  val dateTime: ZonedDateTime = LocalDateTime.now().atZone(ZoneOffset.UTC)
  val year: String = dateTime.format(DateTimeFormatter.ofPattern("yyyy"))
  val month: String = dateTime.format(DateTimeFormatter.ofPattern("MM"))
  val timestamp: String = dateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)
  val outDir: String =  "/" + year + "/" + month
}
