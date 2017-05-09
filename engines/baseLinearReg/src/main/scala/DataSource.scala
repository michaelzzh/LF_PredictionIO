package com.laserfiche

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.predictionio.data.storage.Storage

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(val appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

//Read all events involving "point" type 
       println("Gathering data from the event server")


    val training_points: RDD[LabeledPoint] = PEventStore.aggregateProperties(
      
      appName = dsp.appName,
      entityType = "user",

      // only keep entities with these required properties defined
      required = Some(List("label", "features")))(sc)
      // aggregateProperties() returns RDD pair of
      // entity ID and its aggregated properties
      .map { case (entityId, properties) =>
        try {
	//Converting to Labeled Point as the LinearRegression Algorithm requires
          LabeledPoint(properties.get[Double]("label"),
            Vectors.dense(
              properties.get[Array[Double]]("features")
            )
          )
        } catch {
          case e: Exception => {
            logger.error(s"Failed to get properties ${properties} of" +
              s" ${entityId}. Exception: ${e}.")
            throw e
          }
        }
      }

    new TrainingData(training_points)
  }
}

class TrainingData(
  val training_points: RDD[LabeledPoint]
) extends Serializable
