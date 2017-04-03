package com.laserfiche

import org.apache.predictionio.controller.EngineFactory
import org.apache.predictionio.controller.Engine

class Query(
  val features : Array[Double]
) extends Serializable

class PredictedResult(
  val label: Double,
  val confidence: Double
) extends Serializable

class ActualResult(
  val label: Double
) extends Serializable

object ClassificationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("naive" -> classOf[NaiveBayesAlgorithm]),
      classOf[Serving])
  }
}