package me.assignment.data

import me.assignment.AssignmentJob
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import collection.JavaConverters.seqAsJavaListConverter

private[assignment] object WeekTags {

  private val weekRange = 1 to 52

  lazy val weekTagDF: DataFrame = {
    val tags = weekRange.map(nr => Row(s"W$nr"))
    AssignmentJob.spark.createDataFrame(tags.asJava, StructType(Seq(
      StructField("weekTag", StringType)
    )))
  }

}
