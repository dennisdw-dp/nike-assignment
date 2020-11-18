package me.assignment.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

private[assignment] class ProductData(rawData: DataFrame) {

  lazy val dataFrame: DataFrame = {
    rawData.select(
      col("productid").cast(StringType).alias("productId"),
      col("division").cast(StringType),
      col("gender").cast(StringType),
      col("category").cast(StringType)
    )
  }

}
