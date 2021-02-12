package utils

object Values {
  def checkIfNull(value: Any): String = {
    value match {
      case Some(v) => v.toString
      case None => null
    }
  }
}
