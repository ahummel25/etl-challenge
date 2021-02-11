
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import scala.concurrent.Await
import slick.collection.heterogeneous.HNil
import slick.jdbc.SQLiteProfile.api._

import DB.getDBConn

case class PermitsTable(
                         id: String, permit: String, permit_type: String, review_type: String, application_start_date: String, issue_date: String,
                         processing_time: String, street_number: String, street_direction: String, street_name: String, suffix: String, work_description: String,
                         building_fee_paid: String, zoning_fee_paid: String, other_fee_paid: String, subtotal_paid: String, building_fee_unpaid: String, zoning_fee_unpaid: String,
                         other_fee_unpaid: String, subtotal_unpaid: String, building_fee_waived: String, zoning_fee_waived: String, other_fee_waived: String, subtotal_waived: String,
                         total_fee: String, contact_1_type: String, contact_1_name: String, contact_1_city: String, contact_1_state: String, contact_1_zipcode: String,
                         reported_cost: String, pin1: String, community_area: String, census_tract: String, ward: String, xcoordinate: String,
                         ycoordinate: String, latitude: String, longitude: String, location: String, computed_region_rpca_8um6: String, computed_region_vrxf_vc4k: String,
                         computed_region_6mkv_f3dw: String, computed_region_bdys_3d7i: String, computed_region_43wa_7qmu: String, computed_region_awaf_s7ux: String
                       )

// Definition of the Permits table
class Permits(tag: Tag) extends Table[PermitsTable](tag, "permits") {
  def id = column[String]("id", O.PrimaryKey)
  def permit = column[String]("permit")
  def permit_type = column[String]("permit_type")
  def review_type = column[String]("review_type")
  def application_start_date = column[String]("application_start_date")
  def issue_date = column[String]("issue_date")
  def processing_time = column[String]("processing_time")
  def street_number = column[String]("street_number")
  def street_direction = column[String]("street_direction")
  def street_name = column[String]("street_name")
  def suffix = column[String]("suffix")
  def work_description = column[String]("work_description")
  def building_fee_paid = column[String]("building_fee_paid")
  def zoning_fee_paid = column[String]("zoning_fee_paid")
  def other_fee_paid = column[String]("other_fee_paid")
  def subtotal_paid = column[String]("subtotal_paid")
  def building_fee_unpaid = column[String]("building_fee_unpaid")
  def zoning_fee_unpaid = column[String]("zoning_fee_unpaid")
  def other_fee_unpaid = column[String]("other_fee_unpaid")
  def subtotal_unpaid = column[String]("subtotal_unpaid")
  def building_fee_waived = column[String]("building_fee_waived")
  def zoning_fee_waived = column[String]("zoning_fee_waived")
  def other_fee_waived = column[String]("other_fee_waived")
  def subtotal_waived = column[String]("subtotal_waived")
  def total_fee = column[String]("total_fee")
  def contact_1_type = column[String]("contact_1_type")
  def contact_1_name = column[String]("contact_1_name")
  def contact_1_city = column[String]("contact_1_city")
  def contact_1_state = column[String]("contact_1_state")
  def contact_1_zipcode = column[String]("contact_1_zipcode")
  def reported_cost = column[String]("reported_cost")
  def pin1 = column[String]("pin1")
  def community_area = column[String]("community_area")
  def census_tract = column[String]("census_tract")
  def ward = column[String]("ward")
  def xcoordinate = column[String]("xcoordinate")
  def ycoordinate = column[String]("ycoordinate")
  def latitude = column[String]("latitude")
  def longitude = column[String]("longitude")
  def location = column[String]("location")
  def computed_region_rpca_8um6 = column[String]("computed_region_rpca_8um6")
  def computed_region_vrxf_vc4k = column[String]("computed_region_vrxf_vc4k")
  def computed_region_6mkv_f3dw = column[String]("computed_region_6mkv_f3dw")
  def computed_region_bdys_3d7i = column[String]("computed_region_bdys_3d7i")
  def computed_region_43wa_7qmu = column[String]("computed_region_43wa_7qmu")
  def computed_region_awaf_s7ux = column[String]("computed_region_awaf_s7ux")
  // Every table needs a * projection with the same types as the table's type parameter
  def * = (
      id :: permit :: permit_type :: review_type :: application_start_date :: issue_date ::
      processing_time :: street_number :: street_direction :: street_name :: suffix :: work_description ::
      building_fee_paid :: zoning_fee_paid :: other_fee_paid :: subtotal_paid :: building_fee_unpaid :: zoning_fee_unpaid ::
      other_fee_unpaid :: subtotal_unpaid :: building_fee_waived :: zoning_fee_waived :: other_fee_waived :: subtotal_waived ::
      total_fee :: contact_1_type :: contact_1_name :: contact_1_city :: contact_1_state :: contact_1_zipcode ::
      reported_cost :: pin1 :: community_area :: census_tract :: ward :: xcoordinate ::
      ycoordinate :: latitude :: longitude :: location :: computed_region_rpca_8um6 :: computed_region_vrxf_vc4k ::
      computed_region_6mkv_f3dw :: computed_region_bdys_3d7i :: computed_region_43wa_7qmu :: computed_region_awaf_s7ux :: HNil
    ).mapTo[PermitsTable]
}

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
      """SELECT id, IFNULL(permit_, ''), IFNULL(permit_type, ''), IFNULL(review_type, ''), IFNULL(application_start_date, ''), IFNULL(issue_date, ''), IFNULL(processing_time, ''), IFNULL(street_number, ''), IFNULL(street_direction, ''),
        |      IFNULL(street_name, ''), IFNULL(suffix, ''), IFNULL(work_description, ''), IFNULL(building_fee_paid, ''), IFNULL(zoning_fee_paid, ''), IFNULL(other_fee_paid, ''), IFNULL(subtotal_paid, ''), IFNULL(building_fee_unpaid, ''),
        |      IFNULL(zoning_fee_unpaid, ''), IFNULL(other_fee_unpaid, ''), IFNULL(subtotal_unpaid, ''), IFNULL(building_fee_waived, ''), IFNULL(zoning_fee_waived, ''), IFNULL(other_fee_waived, ''), IFNULL(subtotal_waived, ''),
        |      IFNULL(total_fee, ''), IFNULL(contact_1_type, ''), IFNULL(contact_1_name, ''), IFNULL(contact_1_city, ''), IFNULL(contact_1_state, ''), IFNULL(contact_1_zipcode, ''), IFNULL(reported_cost, ''), IFNULL(pin1, ''), IFNULL(community_area, ''),
        |      IFNULL(census_tract, ''), IFNULL(ward, ''), IFNULL(xcoordinate, ''), IFNULL(ycoordinate, ''), IFNULL(latitude, ''), IFNULL(longitude, ''), IFNULL(location.type, ''), IFNULL(`:@computed_region_rpca_8um6`, ''), IFNULL(`:@computed_region_vrxf_vc4k`, ''),
        |      IFNULL(`:@computed_region_6mkv_f3dw`, ''), IFNULL(`:@computed_region_bdys_3d7i`, ''), IFNULL(`:@computed_region_43wa_7qmu`, ''), IFNULL(`:@computed_region_awaf_s7ux`, '')
        |FROM """.stripMargin + memoryTableName +
      """ ORDER BY id LIMIT 100""" +
      ""
    val allPermitRecordsDf = spark.sql(query)

    val recentRecordsDf = spark.sql("SELECT * FROM " + memoryTableName + " WHERE CAST(issue_date AS DATE) > '2019-12-31'")
    println("You have " + recentRecordsDf.count() + " new permits issued within the last year")

    val unpaidBuildingFees = spark.sql("SELECT SUM(CAST(building_fee_unpaid AS INT)) as total_unpaid_building_fees FROM " + memoryTableName + " WHERE CAST(building_fee_unpaid AS INT) > 0").first()
    println("You have a total of $" + unpaidBuildingFees.get(0) + " in unpaid building fees")

    // Clear table if data is persisted
    val db = getDBConn()
    val execStmt = db.run(sql"DELETE FROM permits".as[String])
    Await.result(execStmt, 10.seconds)

    val permits = TableQuery[Permits]

//    var inss = scala.collection.mutable.ListBuffer.empty[DBIOAction[Unit, NoStream, Effect.All]]

    for (permit <- allPermitRecordsDf.collect()) {
      val ins: DBIOAction[Unit, NoStream, Effect.All] = DBIO.seq(
        permits += PermitsTable (
          permit.get(0).toString, permit.get(1).toString, permit.get(2).toString, permit.get(3).toString,
          permit.get(4).toString, permit.get(5).toString, permit.get(6).toString, permit.get(7).toString,
          permit.get(8).toString, permit.get(9).toString, permit.get(10).toString, permit.get(11).toString,
          permit.get(12).toString, permit.get(13).toString, permit.get(14).toString, permit.get(15).toString,
          permit.get(16).toString, permit.get(17).toString, permit.get(18).toString, permit.get(19).toString,
          permit.get(20).toString, permit.get(21).toString, permit.get(22).toString, permit.get(23).toString,
          permit.get(24).toString, permit.get(25).toString, permit.get(26).toString, permit.get(27).toString,
          permit.get(28).toString, permit.get(29).toString, permit.get(30).toString, permit.get(31).toString,
          permit.get(32).toString, permit.get(33).toString, permit.get(34).toString, permit.get(35).toString,
          permit.get(36).toString, permit.get(37).toString, permit.get(38).toString, permit.get(39).toString,
          permit.get(40).toString, permit.get(41).toString, permit.get(42).toString, permit.get(43).toString,
          permit.get(44).toString, permit.get(45).toString
        )
      )
//      inss += ins
      Await.result(db.run(ins), 10.seconds)
    }

    db.close()
    spark.close()
  }
}
