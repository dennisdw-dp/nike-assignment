package me.assignment.conf

import io.circe.generic.JsonCodec

@JsonCodec sealed trait Granularity

case object Weekly extends Granularity
case object Yearly extends Granularity
