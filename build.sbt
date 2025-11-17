val SPARK_VERSION = "3.5.5"
val HADOOP_VERSION = "3.4.1"

name := "batch-process-dpla-index"
version := "0.1"
scalaVersion := "2.12.18"
organization := "dpla"

assembly / assemblyJarName := "batch-process-dpla-index-assembly.jar"
assembly / mainClass := Some("dpla.batch_process_dpla_index.processes.AllProcessesEntry")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _      => MergeStrategy.concat
      case _                    => MergeStrategy.discard
    }
  case "application.conf"       => MergeStrategy.concat
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-hadoop-cloud" % SPARK_VERSION,
  "org.apache.spark" %% "spark-avro" % SPARK_VERSION,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,
)