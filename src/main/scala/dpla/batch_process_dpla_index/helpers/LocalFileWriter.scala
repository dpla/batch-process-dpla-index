package dpla.batch_process_dpla_index.helpers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

trait LocalFileWriter {

  def writeLocal(outpath: String, name: String, data: String): String = {
    val outputDir = new File(outpath)
    val outputFile = new File(outputDir, name)
    Files.write(outputFile.toPath, data.getBytes(StandardCharsets.UTF_8))
    outputFile.getName
  }
}
