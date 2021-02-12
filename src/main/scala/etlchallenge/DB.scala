package etlchallenge

import slick.jdbc.SQLiteProfile.api._

object DB {
  def getDBConn(): Database = Database.forConfig("etl")
}
