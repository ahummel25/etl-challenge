package etlchallenge.domain

import slick.collection.heterogeneous.HNil
import slick.jdbc.SQLiteProfile.api._

case class PermitsTable(
                         id: Int, permit: String, permit_type: String, review_type: String, application_start_date: String, issue_date: String,
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
  def id = column[Int]("id", O.PrimaryKey)

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
