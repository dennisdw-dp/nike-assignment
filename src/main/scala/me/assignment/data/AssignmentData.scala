package me.assignment.data

import org.apache.spark.sql.DataFrame

private[assignment] trait AssignmentData {

  val salesDF: DataFrame
  val calendarDF: DataFrame
  val storeDF: DataFrame
  val productDF: DataFrame

}
