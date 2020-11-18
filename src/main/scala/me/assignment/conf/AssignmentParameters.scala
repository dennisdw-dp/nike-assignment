package me.assignment.conf

import io.circe.Decoder
import io.circe.generic.JsonCodec

@JsonCodec case class AssignmentParameters(input: InputParameters, divisions: Seq[String], granularity: Granularity, output: OutputParameters)
