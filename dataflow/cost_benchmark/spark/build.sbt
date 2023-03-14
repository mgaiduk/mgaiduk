scalaVersion := "2.12.10"

name := "word-count"
organization := "mgaiduk"
version := "1.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided"
)


