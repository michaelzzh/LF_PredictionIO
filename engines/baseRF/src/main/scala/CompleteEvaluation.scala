package com.laserfiche

import org.apache.predictionio.controller.Evaluation
import org.apache.predictionio.controller.MetricEvaluator

object CompleteEvaluation extends Evaluation {
  engineEvaluator = (
    RFClassificationEngine(),
    MetricEvaluator(
      metric = Accuracy(),
      otherMetrics = Seq(Precision(0.0), Precision(1.0), Precision(2.0)),
      outputPath = "best.json"))
}