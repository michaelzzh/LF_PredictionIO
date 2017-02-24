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

package org.apache.predictionio.examples.java.parallel;

import org.apache.predictionio.controller.java.EmptyParams;
import org.apache.predictionio.controller.java.PJavaDataSource;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple3;

public class DataSource extends PJavaDataSource<
  EmptyParams, Object, JavaPairRDD<String, Float>, Query, Object> {

  final static Logger logger = LoggerFactory.getLogger(DataSource.class);

  public DataSource() {
  }

  @Override
  public Iterable<Tuple3<Object, JavaPairRDD<String, Float>, JavaPairRDD<Query, Object>>>
      read(JavaSparkContext jsc) {
    JavaPairRDD<String, Float> readings = jsc.textFile("../data/helloworld/data.csv")
      .mapToPair(new PairFunction<String, String, Float>() {
        @Override
        public Tuple2 call(String line) {
          String[] tokens = line.split("[\t,]");
          Tuple2 reading = null;
          try {
            reading = new Tuple2(
              tokens[0],
              Float.parseFloat(tokens[1]));
          } catch (Exception e) {
            logger.error("Can't parse reading file. Caught Exception: " + e.getMessage());
            System.exit(1);
          }
          return reading;
        }
      });

    List<Tuple3<Object, JavaPairRDD<String, Float>, JavaPairRDD<Query, Object>>> data =
      new ArrayList<>();

    data.add(new Tuple3(
      null,
      readings,
      jsc.parallelizePairs(new ArrayList<Tuple2<Query, Object>>())
    ));

    return data;
  }
}
