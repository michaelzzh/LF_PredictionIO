package com.laserfiche

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params


import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.SparkContext
import grizzled.slf4j.Logger




case class AlgorithmParams(
//Whether the model should train with an intercept
  val intercept : Double
) extends Params


// extends P2LAlgorithm if Model contains RDD[]

class LinearRegAlgorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, LinearRegressionModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc:SparkContext, data: PreparedData): LinearRegressionModel = {
    // MLLib Linear Regression cannot handle empty training data.
    require(!data.training_points.take(1).isEmpty,
      s"RDD[labeldPoints] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    val lin = new LinearRegressionWithSGD() 

    


    //It is set to True only in the intercept field is set to 1
    //Right now, I am inputting this parameter as an integer, could be changed to String or Bool as necessary
    
    lin.setIntercept(ap.intercept.equals(1.0))
    lin.optimizer.setStepSize(0.005)
    lin.run(data.training_points)
  }

  def predict(model: LinearRegressionModel, query: Query): PredictedResult = {
  
    val result = model.predict(Vectors.dense(query.features))
    new PredictedResult(result)
  }

}
