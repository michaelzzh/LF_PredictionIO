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


package org.apache.predictionio.tools.console.api

import java.util.ServiceLoader

import akka.event.LoggingAdapter
import grizzled.slf4j.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable

class ConsoleEventServerPluginContext(
    val plugins: mutable.Map[String, mutable.Map[String, ConsoleEventServerPlugin]],
    val log: LoggingAdapter) {
  def inputBlockers: Map[String, ConsoleEventServerPlugin] =
    plugins.getOrElse(ConsoleEventServerPlugin.inputBlocker, Map()).toMap

  def inputSniffers: Map[String, ConsoleEventServerPlugin] =
    plugins.getOrElse(ConsoleEventServerPlugin.inputSniffer, Map()).toMap
}

object ConsoleEventServerPluginContext extends Logging {
  def apply(log: LoggingAdapter): ConsoleEventServerPluginContext = {
    val plugins = mutable.Map[String, mutable.Map[String, ConsoleEventServerPlugin]](
      ConsoleEventServerPlugin.inputBlocker -> mutable.Map(),
      ConsoleEventServerPlugin.inputSniffer -> mutable.Map())
    val serviceLoader = ServiceLoader.load(classOf[ConsoleEventServerPlugin])
    serviceLoader foreach { service =>
      plugins(service.pluginType) += service.pluginName -> service
    }
    new ConsoleEventServerPluginContext(
      plugins,
      log)
  }
}
