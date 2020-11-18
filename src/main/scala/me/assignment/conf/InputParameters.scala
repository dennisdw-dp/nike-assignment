package me.assignment.conf

import io.circe.generic.JsonCodec
import me.assignment.data.{AssignmentData, LocalAssignmentData}
import org.apache.spark.sql.DataFrame

@JsonCodec sealed trait InputParameters {
  def getData: AssignmentData
}

@JsonCodec case class LocalInput(basePath: String, salesFile: String, calendarFile: String,
                                 storeFile: String, productFile: String) extends InputParameters {
  override def getData: AssignmentData = new LocalAssignmentData(basePath, salesFile, calendarFile, storeFile, productFile)
}

@JsonCodec private[assignment] case class TestInput() extends InputParameters {
  override def getData: AssignmentData = new AssignmentData {
    override val salesDF: DataFrame = null
    override val calendarDF: DataFrame = null
    override val storeDF: DataFrame = null
    override val productDF: DataFrame = null
  }
}