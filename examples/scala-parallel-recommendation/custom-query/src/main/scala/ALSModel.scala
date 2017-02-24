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

package org.template.recommendation

import org.apache.predictionio.controller.IPersistentModel
import org.apache.predictionio.controller.IPersistentModelLoader
import org.apache.predictionio.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ALSModel(
  val productFeatures: RDD[(Int, Array[Double])],
  val itemStringIntMap: BiMap[String, Int],
  // HOWTO: added a map of `generatedItemIntId -> Item` to the algo data model.
  val items: Map[Int, Item])
  extends IPersistentModel[ALSAlgorithmParams] with Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  def save(id: String, params: ALSAlgorithmParams,
           sc: SparkContext): Boolean = {

    productFeatures.saveAsObjectFile(s"/tmp/${id}/productFeatures")
    sc.parallelize(Seq(itemStringIntMap))
      .saveAsObjectFile(s"/tmp/${id}/itemStringIntMap")
    // HOWTO: save items too as part of algo model
    sc.parallelize(Seq(items))
      .saveAsObjectFile(s"/tmp/${id}/items")
    true
  }

  override def toString = {
    s" productFeatures: [${productFeatures.count()}]" +
      s"(${productFeatures.take(2).toList}...)" +
      s" itemStringIntMap: [${itemStringIntMap.size}]" +
      s"(${itemStringIntMap.take(2).toString}...)]" +
      s" items: [${items.size}]" +
      s"(${items.take(2).toString}...)]"
  }
}

object ALSModel extends IPersistentModelLoader[ALSAlgorithmParams, ALSModel] {
  def apply(id: String, params: ALSAlgorithmParams, sc: Option[SparkContext]) =
    new ALSModel(
      productFeatures = sc.get.objectFile(s"/tmp/${id}/productFeatures"),
      itemStringIntMap = sc.get
        .objectFile[BiMap[String, Int]](s"/tmp/${id}/itemStringIntMap").first,
    // HOWTO: read items too as part of algo model
      items = sc.get
        .objectFile[Map[Int, Item]](s"/tmp/${id}/items").first)
}
