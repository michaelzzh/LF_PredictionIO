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

package pio.refactor

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class AlgorithmParams(mult: Int) extends Params

class Algorithm(val ap: AlgorithmParams)
  // extends PAlgorithm if Model contains RDD[]
  extends P2LAlgorithm[TrainingData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(data: TrainingData): Model = {
    // Simply count number of events
    // and multiple it by the algorithm parameter
    // and store the number as model
    val count = data.events.reduce(_ + _) * ap.mult
    logger.error("Algo.train")
    new Model(mc = count)
  }

  def predict(model: Model, query: Query): PredictedResult = {
    // Prefix the query with the model data
    //val result = s"${model.mc}-${query.q}"
    logger.error("Algo.predict")
    PredictedResult(p = model.mc + query.q)
  }
}

class Model(val mc: Int) extends Serializable {
  override def toString = s"mc=${mc}"
}
