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

package org.apache.predictionio.examples.friendrecommendation

import scala.collection.immutable.HashMap

class KeywordSimilarityModel (
  // External-internal id map
  val userIdMap: HashMap[Int, Int],
  val itemIdMap: HashMap[Int, Int],
  // Keyword array, internal id index, term-weight map item
  val userKeyword: Array[HashMap[Int, Double]],
  val itemKeyword: Array[HashMap[Int, Double]],
  // Weight and threshold trained
  val keywordSimWeight: Double,
  val keywordSimThreshold: Double
) extends Serializable
