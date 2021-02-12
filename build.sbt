name := "Permits-Load"
version := "0.1"
scalaVersion := "2.12.13"

val sparkVersion = "3.0.1"
val scalaTestVersion    = "3.2.2"
val scalaCheckVersion   = "1.14.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe.slick" %% "slick" % "3.3.3",
  "org.xerial" % "sqlite-jdbc" % "3.7.2",

  "org.scalatest" %% "scalatest" % scalaTestVersion  % "test",
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
)

fork := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

addCommandAlias("ETL", "runMain etlchallenge.PermitsETL")
