package com.laserfiche

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import scala.math._
import grizzled.slf4j.Logger

case class AlgorithmParams(
  lambda: Double
) extends Params

// extends P2LAlgorithm because the MLlib's NaiveBayesModel doesn't contain RDD.
class NaiveBayesAlgorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, NaiveBayesModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  private def innerProduct (x : Array[Double], y : Array[Double]) : Double = {
    x.zip(y).map(e => e._1 * e._2).sum
  }

  val normalize = (u: Array[Double]) => {
    val uSum = u.sum

    u.map(e => e / uSum)
  }

  def train(sc: SparkContext, data: PreparedData): NaiveBayesModel = {
    // MLLib NaiveBayes cannot handle empty training data.
    require(data.labeledPoints.take(1).nonEmpty,
      s"RDD[labeledPoints] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preparator generates PreparedData correctly.")

    NaiveBayes.train(data.labeledPoints, ap.lambda)
  }

  def predict(model: NaiveBayesModel, query: Query): PredictedResult = {
    val probabilityArray: Array[(Double,Array[Double])] = model.pi.zip(model.theta)
    val scoreArray: Array[Double] = probabilityArray
      .map(e => innerProduct(e._2, query.features.toArray) + e._1)
    
    val x: Array[Double] = normalize((0 until scoreArray.size).map(k => exp(scoreArray(k) - scoreArray.max)).toArray)
    val y: (Double, Double) = (model.labels zip x).maxBy(_._2)
    new PredictedResult(y._1, y._2)
  }

}