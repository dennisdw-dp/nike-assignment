package me.assignment.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

private[assignment] class SalesData(private val rawData: DataFrame) {

  lazy val dataFrame: DataFrame = {
    // Convert the strings in the csv to the correct types
    rawData.select(
      col("saleId").cast(IntegerType),
      col("netSales").cast(DoubleType),
      col("salesUnits").cast(IntegerType),
      col("storeId").cast(IntegerType),
      col("dateId").cast(IntegerType),
      col("productId").cast(StringType)
    )
  }

}
