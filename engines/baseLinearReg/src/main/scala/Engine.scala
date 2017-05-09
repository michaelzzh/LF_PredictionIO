
package com.laserfiche


import org.apache.predictionio.controller.IEngineFactory
import org.apache.predictionio.controller.Engine

class Query(
  val features: Array[Double]
) extends Serializable

class PredictedResult(
  val label: Double
) extends Serializable

object LinearRegressionEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("linearReg" -> classOf[LinearRegAlgorithm]),
      classOf[Serving])
  }
}
