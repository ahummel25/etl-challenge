package etlchallenge.utils

import org.apache.spark.SparkContext

object LogLevel extends Enumeration {
  val INFO: LogLevel.Value = Value("INFO")
  val WARN: LogLevel.Value = Value("WARN")
  val ERROR: LogLevel.Value = Value("ERROR")

  def setLogLevel(sc: SparkContext, logLevel: LogLevel.Value): Unit = {
    sc.setLogLevel(logLevel.toString)
  }
}
