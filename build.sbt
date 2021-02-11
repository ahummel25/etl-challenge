name := "HDFS-Load"
version := "0.1"
scalaVersion := "2.12.13"

val sparkVersion = "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.typesafe.slick" %% "slick" % "3.3.3",
  "org.xerial" % "sqlite-jdbc" % "3.7.2",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.3"
)

fork := true

addCommandAlias("ETL", "runMain PermitsETL")
addCommandAlias("ETLPrint", "runMain PermitsETL print")
