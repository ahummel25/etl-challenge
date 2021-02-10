
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import slick.jdbc.SQLiteProfile.api._

import DB.{getDBConn, runStmt}

object PermitsETL {
  def main(args: Array[String]): Unit = {
    val url = "https://data.cityofchicago.org/resource/building-permits.json"
    val memoryTableName = "permits"

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("ETLChallenge")

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val result = scala.io.Source.fromURL(url).mkString
    val jsonResponseOneLine = result.stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil).

    val df = spark.read.json(jsonRdd)
    df.createOrReplaceTempView(memoryTableName)

    val recentRecordsDf = spark.sql("SELECT * FROM " + memoryTableName + " WHERE CAST(issue_date AS DATE) > '2019-12-31'")
    println("You have " + recentRecordsDf.count() + " new permits issued within the last year")

    val unpaidBuildingFees = spark.sql("SELECT SUM(CAST(building_fee_unpaid AS INT)) as total_unpaid_building_fees FROM " + memoryTableName + " WHERE CAST(building_fee_unpaid AS INT) > 0").first()
    println("You have a total of $" + unpaidBuildingFees.get(0) + " in unpaid building fees within the last year")

    // Drop table if persisted
    val db = getDBConn()
    runStmt(db, "DROP TABLE IF EXISTS permits;")

    // Add inserts here

    db.close()
    spark.close()
  }
}
