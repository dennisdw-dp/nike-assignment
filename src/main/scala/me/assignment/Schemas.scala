package me.assignment

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}

private[assignment] object Schemas {

  val dataRowsSchema: StructType = StructType(Seq(
    StructField("dataRowId", StringType),
    StructField("dataRow", StructType(
      (1 to 52)
        .map(nr => s"W$nr")
        .map(week => StructField(week, DoubleType))
    ))
  ))

  val consumptionSchema: StructType = StructType(Seq(
    StructField("uniqueKey", StringType),
    StructField("division", StringType),
    StructField("gender", StringType),
    StructField("category", StringType),
    StructField("channel", StringType),
    StructField("year", IntegerType),
    StructField("dataRows", dataRowsSchema)
  ))

}
