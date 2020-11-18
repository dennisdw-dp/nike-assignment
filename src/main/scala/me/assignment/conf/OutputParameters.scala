package me.assignment.conf

import io.circe.generic.JsonCodec
import org.apache.spark.sql.DataFrame

@JsonCodec sealed trait OutputParameters {
  def processResults(dataFrame: DataFrame): Unit
}

@JsonCodec case class LocalOutput(path: String, format: String) extends OutputParameters {
  override def processResults(dataFrame: DataFrame): Unit = dataFrame
    .write
    .format(format)
    .save(path)
}
