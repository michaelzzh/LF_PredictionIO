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

package org.apache.predictionio.examples.stock

import org.apache.predictionio.controller.Params
import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.LDataSource
import org.apache.predictionio.controller.EmptyParams

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import com.mongodb.casbah.Imports._
import org.saddle._
import org.saddle.index.IndexTime
import com.github.nscala_time.time.Imports._


/** Primary parameter for [[[DataSource]]].
  *
  * @param baseDate identify the beginning of our global time window, and
  * the rest are use index.
  * @param fromIdx the first date for testing
  * @param untilIdx the last date (exclusive) for testing
  * @param trainingWindowSize number of days used for training
  * @param testingWindowSize  number of days for each testing data
  *
  * [[[DataSource]]] chops data into (overlapping) multiple
  * pieces. Within each piece, it is further splitted into training and testing
  * set. The testing sets is from <code>fromIdx</code> until
  * <code>untilIdx</code> with a step size of <code>testingWindowSize</code>.
  * A concrete example: (from, until, windowSize) = (100, 150, 20), it generates
  * three testing sets corresponding to time range: [100, 120), [120, 140), [140, 150).
  * For each testing sets, it also generates the training data set using
  * <code>maxTrainingWindowSize</code>. Suppose trainingWindowSize = 50 and testing set =
  * [100, 120), the training set draws data in time range [50, 100).
  */

case class DataSourceParams(
  val appid: Int = 1008,
  val baseDate: DateTime,
  val fromIdx: Int,
  val untilIdx: Int,
  val trainingWindowSize: Int,
  val maxTestingWindowSize: Int,
  val marketTicker: String,
  val tickerList: Seq[String]) extends Params {}
