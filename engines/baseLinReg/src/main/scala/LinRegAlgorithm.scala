package com.laserfiche

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.math._
import grizzled.slf4j.Logger
import scala.math._

case class AlgorithmParams(
	val intercept : Double
) extends Params

class LinRegAlgorithm(val ap: AlgorithmParams)
	extends P2LAlgorithm[PreparedData, LinearRegressionModel, Query, PredictedResult] {

	@transient lazy val logger = Logger[this.type]

	def train(sc:SparkContext, data:PreparedData): LinearRegressionModel = {
		require(!data.training_points.take(1).isEmpty,
			s"RDD[labeledPoint] in PreparedData cannot be empty." +
			"Please check if DataSource generates TrainingData" +
      		" and Preparator generates PreparedData correctly")
		val lin = new LinearRegressionWithSGD()

		lin.setIntercept(ap.intercept.equals(1))
		lin.run(data.training_points)
	}

	def predict(model: LinearRegressionModel, query: Query):PredictedResult = {
		val result = model.predict(Vectors.dense(query.features))
		new PredictedResult(result)
	}
}