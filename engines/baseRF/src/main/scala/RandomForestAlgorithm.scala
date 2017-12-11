package com.laserfiche

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import scala.math._
import grizzled.slf4j.Logger
import scala.math._
import java.io._

case class AlgorithmParams(
  numClasses: Int,
  numTrees: Int,
  featureSubsetStrategy: String,
  impurity: String,
  maxDepth: Int,
  maxBins: Int
) extends Params

class RandomForestAlgorithm(val ap: AlgorithmParams) // CHANGED
  extends P2LAlgorithm[PreparedData, RandomForestModel, // CHANGED
  Query, PredictedResult] {

  // CHANGED
  def train(sc: SparkContext, data: PreparedData): RandomForestModel = {
    // CHANGED
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    var m = RandomForest.trainClassifier(
      data.labeledPoints,
      ap.numClasses,
      categoricalFeaturesInfo,
      ap.numTrees,
      ap.featureSubsetStrategy,
      ap.impurity,
      ap.maxDepth,
      ap.maxBins)
    val file = new File("decision_trees.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(m.toDebugString)
    bw.write("--------------\n")
    bw.close()
    m
  }

  def predict(
    model: RandomForestModel, // CHANGED
    query: Query): PredictedResult = {
    val label = model.predict(Vectors.dense(
      query.features.toArray
    ))
    new PredictedResult(label)
  }

}
