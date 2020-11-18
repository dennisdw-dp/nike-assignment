package me.assignment.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

private[assignment] class CalendarData(private val rawData: DataFrame) {

  lazy val dataFrame: DataFrame = {
    // Convert the strings in the csv to the correct types
    val typedDF = rawData.select(
      col("datekey").cast(IntegerType).alias("dateId"),
      col("datecalendarday").cast(IntegerType).alias("dayOfMonth"),
      col("datecalendaryear").cast(IntegerType).alias("year"),
      col("weeknumberofseason").cast(IntegerType).alias("weekNumber")
    )

    // Add the week id, in the format W#, where # is the week number.

    // The source data does not contain the quarter/season the data belongs to.
    // Assuming the data is in the correct order, it could be inferred from that information
    // (ie, the first occurrence of a week number of the year is the first quarter, the second is the second quarter
    //  and so the 'real' week number would be 13 + weekNumber)
    // For now, just ignore the quarters and use the week numbers between 1-13
    typedDF.join(
      WeekTags.weekTagDF,
      concat(lit("W"), col("weekNumber")).eqNullSafe(WeekTags.weekTagDF("weekTag")),
      "inner")
  }

}
