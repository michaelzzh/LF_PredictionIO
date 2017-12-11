package com.laserfiche

import org.apache.predictionio.controller.AverageMetric
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EngineParams
import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.Evaluation
import org.apache.predictionio.controller.OptionAverageMetric
import scala.collection.mutable.ListBuffer

import scala.io.Source

case class Accuracy()
  extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  def calculate(query: Query, predicted: PredictedResult, actual: ActualResult)
  : Double = 
  {
    (if (predicted.label == actual.label) 1.0 else 0.0)
  }
}

object AccuracyEvaluation extends Evaluation {
  // Define Engine and Metric used in Evaluation
  engineMetric = (RFClassificationEngine(), Accuracy())
}

case class Recall()
  extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  def calculate(query: Query, predicted: PredictedResult, actual: ActualResult)
  : Option[Double] = 
  {
    if (actual.label == 1.0) 
    {
      if (predicted.label == 1.0)
      {
        Some(1.0)
      }
      else
      {
        Some(0.0)
      }
    }
    else
    {
      None
    }
  }
}

object RecallEvaluation extends Evaluation {
  // Define Engine and Metric used in Evaluation
  engineMetric = (RFClassificationEngine(), Recall())
}


object EngineParamsList extends EngineParamsGenerator {
  val fname = "engineId"
  var eId = ""
  for (line <- Source.fromFile("/home/dev/PredictionIO/LF_PredictionIO/engines/baseRF/" + fname).getLines) {
    eId = line
  }
  // Define list of EngineParams used in Evaluation

  // First, we define the base engine params. It specifies the appId from which
  // the data is read, and a evalK parameter is used to define the
  // cross-validation.
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = eId, evalK = Some(5)))

  // Second, we specify the engine params list by explicitly listing all
  // algorithm parameters. In this case, we evaluate 3 engine params, each with
  // a different algorithm params value.

  var hpfile = "hyperParams"
  var hPs = new ListBuffer[Seq[String]]()
  for (line <- Source.fromFile("/home/dev/PredictionIO/LF_PredictionIO/engines/baseRF/" + hpfile).getLines) {
    hPs += line.replaceAll(" ","").split(",").toList
  }

  def crossProduct(l1: ListBuffer[Seq[String]], l2: Seq[String]) : ListBuffer[Seq[String]] = {
    var runningCP = new ListBuffer[Seq[String]]

    for {i1 <- l1; i2 <- l2} runningCP += i1 ++ List(i2)
    runningCP
  }

  var first = new ListBuffer[Seq[String]]

  for {i <- hPs(0)} first += Seq(i)

  var runningCross = crossProduct(first, hPs(1))

  for {i <- List.range(2, hPs.length)} runningCross = crossProduct(runningCross, hPs(i))

  println(runningCross)

  engineParamsList = for (i <- List.range(0,runningCross.length)) yield baseEP.copy(algorithmParamsList = Seq(("randomforest", AlgorithmParams(runningCross(i)(0).toInt,runningCross(i)(1).toInt,runningCross(i)(2),runningCross(i)(3),runningCross(i)(4).toInt,runningCross(i)(5).toInt))))

  //engineParamsList = Seq(
  //  baseEP.copy(algorithmParamsList = Seq(("randomforest", AlgorithmParams(2,100,"auto","gini",10,50)))))
}

