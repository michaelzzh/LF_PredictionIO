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

package org.apache.predictionio.examples.java.regression;

import org.apache.predictionio.controller.java.EmptyParams;
import org.apache.predictionio.controller.java.IJavaEngineFactory;
import org.apache.predictionio.controller.java.JavaParams;
import org.apache.predictionio.controller.java.JavaEngine;
import org.apache.predictionio.controller.java.JavaEngineBuilder;
import org.apache.predictionio.controller.java.JavaEngineParams;
import org.apache.predictionio.controller.java.JavaEngineParamsBuilder;
import org.apache.predictionio.controller.java.LJavaAlgorithm;
import org.apache.predictionio.controller.java.JavaWorkflow;
import org.apache.predictionio.controller.java.WorkflowParamsBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

public class Run {
  // During development, one can build a semi-engine, only add the first few layers. In this
  // particular example, we only add until Algorithm Layer.
  private static class HalfBakedEngineFactory implements IJavaEngineFactory {
    public JavaEngine<TrainingData, Integer, TrainingData, Double[], Double, Double> apply() {
      return new JavaEngineBuilder<TrainingData, Integer, TrainingData, Double[], Double, Double> ()
        .dataSourceClass(DataSource.class)
        .preparatorClass(Preparator.class)
        .addAlgorithmClass("OLS", OLSAlgorithm.class)
        .addAlgorithmClass("Default", DefaultAlgorithm.class)
        .build();
    }
  }

  public static void runComponents() throws IOException {
    JavaEngineParams engineParams = new JavaEngineParamsBuilder()
      .dataSourceParams(new DataSourceParams(new File("../data/lr_data.txt").getCanonicalPath()))
      .preparatorParams(new PreparatorParams(0.3))
      .addAlgorithmParams("OLS", new EmptyParams())
      .addAlgorithmParams("Default", new DefaultAlgorithmParams(0.2))
      .addAlgorithmParams("Default", new DefaultAlgorithmParams(0.4))
      .build();

    JavaWorkflow.runEngine(
        (new HalfBakedEngineFactory()).apply(),
        engineParams,
        new WorkflowParamsBuilder().batch("java regression engine").verbose(3).build()
        );
  }

  public static void runEngine() throws IOException {
    JavaEngineParams engineParams = new JavaEngineParamsBuilder()
      .dataSourceParams(new DataSourceParams(new File("../data/lr_data.txt").getCanonicalPath()))
      .preparatorParams(new PreparatorParams(0.3))
      .addAlgorithmParams("OLS", new EmptyParams())
      .addAlgorithmParams("Default", new DefaultAlgorithmParams(0.2))
      .addAlgorithmParams("Default", new DefaultAlgorithmParams(0.4))
      .build();

    JavaWorkflow.runEngine(
        (new EngineFactory()).apply(),
        engineParams,
        MeanSquareEvaluator.class,
        new EmptyParams(),
        new WorkflowParamsBuilder().batch("java regression engine").verbose(3).build()
        );
  }

  public static void main(String[] args) {
    try {
      runEngine();
      //runComponents();
    } catch (IOException ex) {
      System.out.println(ex);
    }
  }
}
