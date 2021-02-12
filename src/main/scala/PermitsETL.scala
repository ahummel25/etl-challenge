import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import scala.concurrent.Await
import slick.jdbc.SQLiteProfile.api._

import DB.getDBConn
import domain.{Permits, PermitsTable}
import utils.Values.checkIfNull

object PermitsETL {
  def main(args: Array[String]): Unit = {
    val url = "https://data.cityofchicago.org/resource/building-permits.json"
    val memoryTableName = "permits"

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("ETLChallenge")

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val source = scala.io.Source.fromURL(url)
    val result = try source.mkString finally source.close()
    val jsonResponseOneLine = result.stripLineEnd
    val jsonRdd = spark.sparkContext.parallelize(Seq(jsonResponseOneLine))

    import spark.implicits._

    val ds = spark.createDataset(jsonRdd)
    val df = spark.read.json(ds)
    df.createOrReplaceTempView(memoryTableName)

    val query = "" +
      """SELECT id, permit_, permit_type, review_type, application_start_date, issue_date, processing_time, street_number, street_direction,
        |      street_name, suffix, work_description, building_fee_paid, zoning_fee_paid, other_fee_paid, subtotal_paid, building_fee_unpaid,
        |      zoning_fee_unpaid, other_fee_unpaid, subtotal_unpaid, building_fee_waived, zoning_fee_waived, other_fee_waived, subtotal_waived,
        |      total_fee, contact_1_type, contact_1_name, contact_1_city, contact_1_state, contact_1_zipcode, reported_cost, pin1, community_area,
        |      census_tract, ward, xcoordinate, ycoordinate, latitude, longitude, location.type, `:@computed_region_rpca_8um6`, `:@computed_region_vrxf_vc4k`,
        |      `:@computed_region_6mkv_f3dw`, `:@computed_region_bdys_3d7i`, `:@computed_region_43wa_7qmu`, `:@computed_region_awaf_s7ux`
        |FROM """.stripMargin + memoryTableName +
      ""
    val allPermitRecordsDf = spark.sql(query)

    val recentRecordsDf = spark.sql("SELECT * FROM " + memoryTableName + " WHERE CAST(issue_date AS DATE) > '2019-12-31'")
    println("You have " + recentRecordsDf.count() + " new permits issued within the last year")

    val unpaidBuildingFees = spark.sql("SELECT SUM(CAST(building_fee_unpaid AS INT)) as total_unpaid_building_fees FROM " + memoryTableName + " WHERE CAST(building_fee_unpaid AS INT) > 0").first()
    println("You have a total of $" + unpaidBuildingFees.get(0) + " in unpaid building fees")

    val db = getDBConn()
    val permits = TableQuery[Permits]
    val t1 = System.nanoTime

    // Clear table if data is persisted
    Await.result(db.run(permits.delete), 10.seconds)

    for (permit <- allPermitRecordsDf.collect()) {
      val ins: DBIOAction[Unit, NoStream, Effect.All] = DBIO.seq(
        permits += PermitsTable(
          permit.get(0).toString.toInt, checkIfNull(permit.get(1)), checkIfNull(permit.get(2)), checkIfNull(permit.get(3)),
          checkIfNull(permit.get(4)), checkIfNull(permit.get(5)), checkIfNull(permit.get(6)), checkIfNull(permit.get(7)),
          checkIfNull(permit.get(8)), checkIfNull(permit.get(9)), checkIfNull(permit.get(10)), checkIfNull(permit.get(11)),
          checkIfNull(permit.get(12)), checkIfNull(permit.get(13)), checkIfNull(permit.get(14)), checkIfNull(permit.get(15)),
          checkIfNull(permit.get(16)), checkIfNull(permit.get(17)), checkIfNull(permit.get(18)), checkIfNull(permit.get(19)),
          checkIfNull(permit.get(20)), checkIfNull(permit.get(21)), checkIfNull(permit.get(22)), checkIfNull(permit.get(23)),
          checkIfNull(permit.get(24)), checkIfNull(permit.get(25)), checkIfNull(permit.get(26)), checkIfNull(permit.get(27)),
          checkIfNull(permit.get(28)), checkIfNull(permit.get(29)), checkIfNull(permit.get(30)), checkIfNull(permit.get(31)),
          checkIfNull(permit.get(32)), checkIfNull(permit.get(33)), checkIfNull(permit.get(34)), checkIfNull(permit.get(35)),
          checkIfNull(permit.get(36)), checkIfNull(permit.get(37)), checkIfNull(permit.get(38)), checkIfNull(permit.get(39)),
          checkIfNull(permit.get(40)), checkIfNull(permit.get(41)), checkIfNull(permit.get(42)), checkIfNull(permit.get(43)),
          checkIfNull(permit.get(44)), checkIfNull(permit.get(45)))
      )
      Await.result(db.run(ins), 10.seconds)
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println("Inserts took " + duration + " seconds")

    db.close()
    spark.close()
  }
}
