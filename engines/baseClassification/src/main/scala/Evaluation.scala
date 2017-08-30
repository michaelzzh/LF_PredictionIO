package com.laserfiche

import org.apache.predictionio.controller.AverageMetric
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EngineParams
import org.apache.predictionio.controller.EngineParamsGenerator
import org.apache.predictionio.controller.Evaluation
import org.apache.predictionio.controller.OptionAverageMetric

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
  engineMetric = (ClassificationEngine(), Accuracy())
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
  engineMetric = (ClassificationEngine(), Recall())
}


object EngineParamsList extends EngineParamsGenerator {
  // Define list of EngineParams used in Evaluation

  // First, we define the base engine params. It specifies the appId from which
  // the data is read, and a evalK parameter is used to define the
  // cross-validation.
  private[this] val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(appName = "8af4740d-c9c6-48a0-a784-93649ca35fa7", evalK = Some(5)))

  // Second, we specify the engine params list by explicitly listing all
  // algorithm parameters. In this case, we evaluate 3 engine params, each with
  // a different algorithm params value.
  engineParamsList = Seq(
    //baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(0.001)))),
    //baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(0.01)))),
    //baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(0.1)))),
    baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(1.0)))))
    //baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(10.0)))),
    //baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(100.0)))),
    //baseEP.copy(algorithmParamsList = Seq(("naive", AlgorithmParams(1000.0)))))
}

