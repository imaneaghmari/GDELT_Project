name := "Projet_GDELT"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  // Spark dependencies. Marked as provided because they must not be included in the uber jar
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  //Cassandra connection
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.2",
// Third-party libraries
  "org.apache.hadoop" % "hadoop-aws" % "2.6.0" % "provided",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"
  //"com.github.scopt" %% "scopt" % "3.4.0"        // to parse options given to the jar in the spark-submit
)