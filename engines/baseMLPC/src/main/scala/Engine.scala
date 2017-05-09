package com.laserfiche

import org.apache.predictionio.controller.EngineFactory
import org.apache.predictionio.controller.Engine

class Query(
  val features : Array[Double]
) extends Serializable

class Query(
  val features : Array[Double]
) extends Serializable

object MLPCEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("mlpc" -> classOf[MLPC]),
      classOf[Serving])
  }
}