package com.laserfiche

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext

import grizzled.slf4j.Logger
import scala.math._

case class AlgorithmParams(
  lambda: Double
) extends Params

// extends P2LAlgorithm because the MLlib's NaiveBayesModel doesn't contain RDD.
class NaiveBayesAlgorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, NaiveBayesModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): NaiveBayesModel = {
    // MLLib NaiveBayes cannot handle empty training data.
    require(data.labeledPoints.take(1).nonEmpty,
      s"RDD[labeledPoints] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preparator generates PreparedData correctly.")

    NaiveBayes.train(data.labeledPoints, ap.lambda)
  }

  private def innerProduct (x : Array[Double], y : Array[Double]) : Double = {
    x.zip(y).map(e => e._1 * e._2).sum
  }

  val normalize = (u: Array[Double]) => {
    val uSum = u.sum

    u.map(e => e / uSum)
  }

  def predict(model: NaiveBayesModel, query: Query): PredictedResult = {
    val scoreArray = model.pi.zip(model.theta)
    //System.out.println("pi: " + model.pi.mkString(","))
    val x = Vectors.dense(query.features)

    val z = scoreArray
      .map(e => innerProduct(e._2, x.toArray) + e._1)

    val scores = normalize((0 until z.size).map(k => exp(z(k) - z.max)).toArray)
    //System.out.println("theta times features:\n" + z.map(_.mkString(",")).mkString("\n"))
    System.out.println("scores: " + scores.mkString(", "))
    val label = model.predict(Vectors.dense(
      query.features)
    )
    new PredictedResult(label = label, confidence = scores.max)
  }

}