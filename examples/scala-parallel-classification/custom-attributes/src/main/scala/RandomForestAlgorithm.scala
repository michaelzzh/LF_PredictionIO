/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.test1

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.mllib.tree.RandomForest // CHANGED
import org.apache.spark.mllib.tree.model.RandomForestModel // CHANGED
import org.apache.spark.mllib.linalg.Vectors

// CHANGED
case class RandomForestAlgorithmParams(
  numClasses: Int,
  numTrees: Int,
  featureSubsetStrategy: String,
  impurity: String,
  maxDepth: Int,
  maxBins: Int
) extends Params

class PIORandomForestModel(
  val gendersMap: Map[String, Double],
  val educationMap: Map[String, Double],
  val randomForestModel: RandomForestModel
) extends Serializable

// extends P2LAlgorithm because the MLlib's RandomForestModel doesn't
// contain RDD.
class RandomForestAlgorithm(val ap: RandomForestAlgorithmParams) // CHANGED
  extends P2LAlgorithm[PreparedData, PIORandomForestModel, // CHANGED
  Query, PredictedResult] {

  def train(data: PreparedData): PIORandomForestModel = { // CHANGED
    // CHANGED
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val m = RandomForest.trainClassifier(
      data.labeledPoints,
      ap.numClasses,
      categoricalFeaturesInfo,
      ap.numTrees,
      ap.featureSubsetStrategy,
      ap.impurity,
      ap.maxDepth,
      ap.maxBins)
   new PIORandomForestModel(
    gendersMap = data.gendersMap,
    educationMap = data.educationMap,
    randomForestModel = m
   )
  }

  def predict(
    model: PIORandomForestModel, // CHANGED
    query: Query): PredictedResult = {
    val gendersMap = model.gendersMap
    val educationMap = model.educationMap
    val randomForestModel = model.randomForestModel
    val label = randomForestModel.predict(
      Vectors.dense(Array(
        gendersMap(query.gender),
        query.age.toDouble,
        educationMap(query.education))
      ))
    new PredictedResult(label)
  }

}
