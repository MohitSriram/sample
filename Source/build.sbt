name := "Question4"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.spark" %% "spark-graphx" % "2.3.1",
  "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
)