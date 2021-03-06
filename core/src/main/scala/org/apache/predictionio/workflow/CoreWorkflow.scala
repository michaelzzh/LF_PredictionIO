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


package org.apache.predictionio.workflow

import org.apache.predictionio.controller.EngineParams
import org.apache.predictionio.controller.Evaluation
import org.apache.predictionio.core.BaseEngine
import org.apache.predictionio.core.BaseEvaluator
import org.apache.predictionio.core.BaseEvaluatorResult
import org.apache.predictionio.data.storage.EngineInstance
import org.apache.predictionio.data.storage.EvaluationInstance
import org.apache.predictionio.data.storage.Model
import org.apache.predictionio.data.storage.Storage

import com.github.nscala_time.time.Imports.DateTime
import grizzled.slf4j.Logger

import scala.language.existentials

/** CoreWorkflow handles PredictionIO metadata and environment variables of
  * training and evaluation.
  */
object CoreWorkflow {
  @transient lazy val logger = Logger[this.type]
  @transient lazy val engineInstances = Storage.getMetaDataEngineInstances
  @transient lazy val evaluationInstances =
    Storage.getMetaDataEvaluationInstances()

  def runTrain[EI, Q, P, A](
      preDeployment: Boolean = false,
      engine: BaseEngine[EI, Q, P, A],
      engineParams: EngineParams,
      engineInstance: EngineInstance,
      env: Map[String, String] = WorkflowUtils.pioEnvVars,
      params: WorkflowParams = WorkflowParams()) {
    logger.debug("Starting SparkContext")
    val mode = "training"

    val batch = if (params.batch.nonEmpty) {
      s"{engineInstance.engineFactory} (${params.batch}})"
    } else {
      engineInstance.engineFactory
    }
    val sc = WorkflowContext(
      batch,
      env,
      params.sparkEnv,
      mode.capitalize)

    try {
      if(!preDeployment){
        val models: Seq[Any] = engine.train(
        sc = sc,
        engineParams = engineParams,
        engineInstanceId = engineInstance.id,
        params = params
      )

      val instanceId = Storage.getMetaDataEngineInstances

      val kryo = KryoInstantiator.newKryoInjection

      logger.info("Inserting persistent model")
      Storage.getModelDataModels.insert(Model(
        id = engineInstance.id,
        models = kryo(models)))
      }

      
      logger.info("Updating engine instance")
      val getMetaDataEngineInstances = Storage.getMetaDataEngineInstances
      engineInstances.update(engineInstance.copy(
        status = "COMPLETED",
        endTime = DateTime.now
        ))
      val engineManifests = Storage.getMetaDataEngineManifests
      engineManifests.get(engineInstance.engineId, engineInstance.engineVersion) map {em =>
        engineManifests.update(em.copy(trainingStatus = "COMPLETED"))
      } getOrElse {
        error("engine manifest not found for ${engineInstance.engineId}")
      }
      if(!preDeployment){
        logger.info("Training completed successfully.")
      }else{
        logger.info(s"Engine ${engineInstance.id} is ready for deploy")
      }
      
    } catch {
      case e @(
          _: StopAfterReadInterruption |
          _: StopAfterPrepareInterruption) => {
        logger.info(s"Training interrupted by $e.")
      }
    } finally {
      logger.debug("Stopping SparkContext")
      sc.stop()
    }
  }

  def runEvaluation[EI, Q, P, A, R <: BaseEvaluatorResult](
      evaluation: Evaluation,
      engine: BaseEngine[EI, Q, P, A],
      engineParamsList: Seq[EngineParams],
      evaluationInstance: EvaluationInstance,
      evaluator: BaseEvaluator[EI, Q, P, A, R],
      env: Map[String, String] = WorkflowUtils.pioEnvVars,
      params: WorkflowParams = WorkflowParams()) {
    logger.info("runEvaluation started")
    logger.debug("Start SparkContext")

    val mode = "evaluation"

    val batch = if (params.batch.nonEmpty) {
      s"{evaluation.getClass.getName} (${params.batch}})"
    } else {
      evaluation.getClass.getName
    }
    val sc = WorkflowContext(
      batch,
      env,
      params.sparkEnv,
      mode.capitalize)
    val evaluationInstanceId = evaluationInstances.insert(evaluationInstance)

    logger.info(s"Starting evaluation instance ID: $evaluationInstanceId")

    val evaluatorResult: BaseEvaluatorResult = EvaluationWorkflow.runEvaluation(
      sc,
      evaluation,
      engine,
      engineParamsList,
      evaluator,
      params)

    if (evaluatorResult.noSave) {
      logger.info(s"This evaluation result is not inserted into database: $evaluatorResult")
    } else {
      val evaluatedEvaluationInstance = evaluationInstance.copy(
        status = "EVALCOMPLETED",

        id = evaluationInstanceId,
        endTime = DateTime.now,
        evaluatorResults = evaluatorResult.toOneLiner,
        evaluatorResultsHTML = evaluatorResult.toHTML,
        evaluatorResultsJSON = evaluatorResult.toJSON
      )

      logger.info(s"Updating evaluation instance with result: $evaluatorResult")

      evaluationInstances.update(evaluatedEvaluationInstance)
    }

    logger.debug("Stop SparkContext")

    sc.stop()

    logger.info("runEvaluation completed")
  }
}


