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

package org.template.recommendation

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

import org.apache.hadoop.conf.Configuration // ADDED
import org.bson.BSONObject // ADDED
import com.mongodb.hadoop.MongoInputFormat // ADDED

case class DataSourceParams( // CHANGED
  host: String,
  port: Int,
  db: String, // DB name
  collection: String // collection name
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    // CHANGED
    val config = new Configuration()
    config.set("mongo.input.uri",
      s"mongodb://${dsp.host}:${dsp.port}/${dsp.db}.${dsp.collection}")

    val mongoRDD = sc.newAPIHadoopRDD(config,
      classOf[MongoInputFormat],
      classOf[Object],
      classOf[BSONObject])

    // mongoRDD contains tuples of (ObjectId, BSONObject)
    val ratings = mongoRDD.map { case (id, bson) =>
      Rating(bson.get("uid").asInstanceOf[String],
        bson.get("iid").asInstanceOf[String],
        bson.get("rating").asInstanceOf[Double])
    }
    new TrainingData(ratings)
  }
}

case class Rating(
  user: String,
  item: String,
  rating: Double
)

class TrainingData(
  val ratings: RDD[Rating]
) extends Serializable {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }
}
