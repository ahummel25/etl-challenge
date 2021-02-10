import slick.jdbc.SQLiteProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration._

object DB {
  def getDBConn(): Database = Database.forConfig("etl")

  def runStmt(db: Database, stmt: String): Unit = {
    val execStmt = db.run(sql"${stmt}".as[String])
    Await.result(execStmt, 10.seconds)
  }
}
