name := "batch-process-dpla-index"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.4.2"
)