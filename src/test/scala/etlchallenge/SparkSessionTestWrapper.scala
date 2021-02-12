package etlchallenge

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("ETL Challenge Test")
      .getOrCreate()
  }
}
