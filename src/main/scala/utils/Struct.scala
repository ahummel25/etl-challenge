package utils

import org.apache.spark.sql.types.{StringType, StructField, StructType, ArrayType}

object Struct {
  def permitsSchema: StructType = {
    StructType(
      Seq(
        StructField("ID", StringType, nullable = false),
        StructField("PERMIT", StringType, nullable = true),
        StructField("PERMIT_TYPE", StringType, nullable = true),
        StructField("WORK_DESCRIPTION", StringType, nullable = true),
        StructField("LOCATION", StructType(
          Seq(
            StructField("TYPE", StringType, nullable = true),
            StructField("TYPE", ArrayType(StringType), nullable = true)
          )
        ), nullable = true)
      )
    )
  }
}
