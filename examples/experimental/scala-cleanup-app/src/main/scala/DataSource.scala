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

package org.apache.predictionio.examples.experimental.cleanupapp

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.storage.Storage
import org.apache.predictionio.workflow.StopAfterReadInterruption

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.github.nscala_time.time.Imports._

import grizzled.slf4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

case class DataSourceParams(
  appId: Int,
  cutoffTime: DateTime
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()
    val lEventsDb = Storage.getLEvents()
    logger.info(s"CleanupApp: $dsp")

    val countBefore = eventsDb.find(
      appId = dsp.appId
    )(sc).count
    logger.info(s"Event count before cleanup: $countBefore")

    val countRemove = eventsDb.find(
      appId = dsp.appId,
      untilTime = Some(dsp.cutoffTime)
    )(sc).count
    logger.info(s"Number of events to remove: $countRemove")

    logger.info(s"Remove events from appId ${dsp.appId}")
    val eventsToRemove: Array[String] = eventsDb.find(
      appId = dsp.appId,
      untilTime = Some(dsp.cutoffTime)
    )(sc).map { case e =>
      e.eventId.getOrElse("")
    }.collect

    var lastFuture: Future[Boolean] = Future[Boolean] {true}
    eventsToRemove.foreach { case eventId =>
      if (eventId != "") {
        lastFuture = lEventsDb.futureDelete(eventId, dsp.appId)
      }
    }
    // No, it's not correct to just wait for the last result.
    // This program only demonstrates how to remove old events.
    Await.result(lastFuture, scala.concurrent.duration.Duration(5, "minutes"))
    logger.info(s"Finish cleaning up events to appId ${dsp.appId}")

    val countAfter = eventsDb.find(
      appId = dsp.appId
    )(sc).count
    logger.info(s"Event count after cleanup: $countAfter")

    throw new StopAfterReadInterruption()
  }
}

class TrainingData(
) extends Serializable {
  override def toString = ""
}
