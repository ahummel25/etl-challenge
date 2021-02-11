import slick.jdbc.SQLiteProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration._

object DB {
  def getDBConn(): Database = Database.forConfig("etl")
}
