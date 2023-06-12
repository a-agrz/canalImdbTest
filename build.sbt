ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

val AkkaVersion = "2.8.1"

lazy val root = (project in file("."))
  .settings(
    name := "canalImdbTest"
  )


libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "6.0.1",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "org.apache.logging.log4j" % "log4j-api-scala_2.13" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0" % Runtime
)