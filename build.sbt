name := "batch-process-dpla-index"

version := "0.1"

scalaVersion := "2.12.20"

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case x => MergeStrategy.first
}

val SPARK_VERSION = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION % Provided,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION % Provided,
  "com.amazonaws" % "aws-java-sdk" % "1.12.397" % Provided,
  //"org.apache.hadoop" % "hadoop-aws" % "2.10.1",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.12.1",
  "com.squareup.okhttp3" % "okhttp" % "4.12.0",
  "org.apache.spark" %% "spark-avro" % "3.5.5"
)
