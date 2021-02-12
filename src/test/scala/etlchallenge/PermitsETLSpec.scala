package etlchallenge

import org.scalatest.funspec.AnyFunSpec

class PermitsETLSpec extends AnyFunSpec with SparkSessionTestWrapper {
  PermitsETL.main(Array.empty[String])
}
