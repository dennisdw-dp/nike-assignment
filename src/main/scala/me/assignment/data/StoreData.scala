package me.assignment.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType}

private[assignment] class StoreData(rawData: DataFrame) {

  lazy val dataFrame: DataFrame = {
    rawData.select(
      col("storeid").cast(IntegerType).alias("storeId"),
      col("channel").cast(StringType),
      col("country").cast(StringType)
    )
  }

}
