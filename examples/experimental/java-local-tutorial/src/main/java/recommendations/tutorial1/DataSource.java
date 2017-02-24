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

package org.apache.predictionio.examples.java.recommendations.tutorial1;

import org.apache.predictionio.controller.java.LJavaDataSource;
import scala.Tuple2;
import scala.Tuple3;
import java.io.File;
import java.io.FileNotFoundException;
import java.lang.Iterable;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSource extends LJavaDataSource<
  DataSourceParams, Object, TrainingData, Query, Object> {

  final static Logger logger = LoggerFactory.getLogger(DataSource.class);

  DataSourceParams params;

  public DataSource(DataSourceParams params) {
    this.params = params;
  }

  @Override
  public Iterable<Tuple3<Object, TrainingData, Iterable<Tuple2<Query, Object>>>> read() {

    File ratingFile = new File(params.filePath);
    Scanner sc = null;

    try {
      sc = new Scanner(ratingFile);
    } catch (FileNotFoundException e) {
      logger.error("Caught FileNotFoundException " + e.getMessage());
      System.exit(1);
    }

    List<TrainingData.Rating> ratings = new ArrayList<TrainingData.Rating>();

    while (sc.hasNext()) {
      String line = sc.nextLine();
      String[] tokens = line.split("[\t,]");
      try {
        TrainingData.Rating rating = new TrainingData.Rating(
          Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]),
          Float.parseFloat(tokens[2]));
        ratings.add(rating);
      } catch (Exception e) {
        logger.error("Can't parse rating file. Caught Exception: " + e.getMessage());
        System.exit(1);
      }
    }

    List<Tuple3<Object, TrainingData, Iterable<Tuple2<Query, Object>>>> data =
      new ArrayList<Tuple3<Object, TrainingData, Iterable<Tuple2<Query, Object>>>>();

    data.add(new Tuple3<Object, TrainingData, Iterable<Tuple2<Query, Object>>>(
      null,
      new TrainingData(ratings),
      new ArrayList<Tuple2<Query, Object>>()
    ));

    return data;
  }

}
