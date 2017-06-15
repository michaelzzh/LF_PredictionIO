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


package org.apache.predictionio.tools.console

import java.io.File
import java.net.URI

import grizzled.slf4j.Logging
import org.apache.predictionio.controller.Utils
import org.apache.predictionio.core.BuildInfo
//import org.apache.predictionio.tools.console.api.ConsoleEventServer
//import org.apache.predictionio.tools.console.api.ConsoleEventServerConfig
import org.apache.predictionio.data.storage
import org.apache.predictionio.data.storage.ServerConfig
import org.apache.predictionio.data.storage.EngineManifest
import org.apache.predictionio.data.storage.EngineManifestSerializer
import org.apache.predictionio.data.storage.hbase.upgrade.Upgrade_0_8_3
import org.apache.predictionio.tools.RegisterEngine
import org.apache.predictionio.tools.RunServer
import org.apache.predictionio.tools.RunWorkflow
import org.apache.predictionio.tools.admin.AdminServer
import org.apache.predictionio.tools.admin.AdminServerConfig
import org.apache.predictionio.tools.dashboard.Dashboard
import org.apache.predictionio.tools.dashboard.DashboardConfig
import org.apache.predictionio.workflow.JsonExtractorOption
import org.apache.predictionio.workflow.JsonExtractorOption.JsonExtractorOption
import org.apache.predictionio.workflow.WorkflowUtils
import org.apache.commons.io.FileUtils
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.read
import org.json4s.native.Serialization.write
import semverfi._

import scala.collection.JavaConversions._
import scala.io.Source
import scala.sys.process._
import scala.util.Random
import scalaj.http.Http
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import sys.process._
import java.security.SecureRandom
import org.apache.commons.codec.binary.Base64

import scopt.Read
import scopt._

// imports for event server

//import org.apache.predictionio.tools.console.ConsoleArgs
//import org.apache.predictionio.tools.console.Console
//import org.apache.predictionio.tools.console.CommonArgs
//import org.apache.predictionio.tools.console.App
//import org.apache.predictionio.tools.console.AppArgs
//import org.apache.predictionio.tools.console.AccessKeyArgs
//import org.apache.predictionio.tools.console.DeployArgs

import org.apache.predictionio.tools.console.api.ConsoleCommon
import org.apache.predictionio.tools.console.api.ChannelRejection
import org.apache.predictionio.tools.console.api.ConsoleEventServerPluginContext
import org.apache.predictionio.tools.console.api.ConsoleEventInfo
import org.apache.predictionio.tools.console.api.ConsolePluginsActor
import akka.event.Logging
import sun.misc.BASE64Decoder

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import org.apache.predictionio.data.{ Utils => SUtils }
import org.apache.predictionio.data.storage.AccessKeys
import org.apache.predictionio.data.storage.Channels
import org.apache.predictionio.data.storage.DateTimeJson4sSupport
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.storage.EventJson4sSupport
import org.apache.predictionio.data.storage.BatchEventsJson4sSupport
import org.apache.predictionio.data.storage.LEvents
import org.apache.predictionio.data.storage.Storage
import org.apache.predictionio.data.storage.EngineData
import org.apache.predictionio.data.storage.QueryData
// import org.apache.predictionio.data.storage.EngineManifest
import org.apache.predictionio.data.storage.EngineInstance
// import org.apache.predictionio.data.storage.ServerConfig
import org.json4s.DefaultFormats
import org.json4s.Formats
import org.json4s.JObject
import org.json4s.native.JsonMethods.parse
//import org.json4s.native.Serialization.write
import org.json4s._
//import org.json4s.native.JsonMethods._
// rename imported class to prevent ambiguous import
import spray.can.{ Http => SHttp }
import spray.http.FormData
import spray.http.MediaTypes
import spray.http.StatusCodes
import spray.httpx.Json4sSupport
import spray.routing._
import spray.routing.authentication.Authentication
import spray.httpx.SprayJsonSupport
import spray.http._
import scalaj.http.HttpOptions

import scala.concurrent.ExecutionContext
//import scala.concurrent.Future
import scala.util.{Try, Success, Failure}

//import sys.process._
import java.io.File
import java.io.PrintWriter
//import scala.io.Source
import scala.util.Properties

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
//import java.security.SecureRandom
import org.apache.commons.codec.binary.Base64
import scala.collection.mutable.Queue

case class ConsoleArgs(
  common: CommonArgs = CommonArgs(),
  build: BuildArgs = BuildArgs(),
  register: RegisterArgs = RegisterArgs(),
  app: AppArgs = AppArgs(),
  accessKey: AccessKeyArgs = AccessKeyArgs(),
  deploy: DeployArgs = DeployArgs(),
  eventServer: EventServerArgs = EventServerArgs(),
  adminServer: AdminServerArgs = AdminServerArgs(),
  dashboard: DashboardArgs = DashboardArgs(),
  upgrade: UpgradeArgs = UpgradeArgs(),
  template: TemplateArgs = TemplateArgs(),
  export: ExportArgs = ExportArgs(),
  imprt: ImportArgs = ImportArgs(),
  commands: Seq[String] = Seq(),
  metricsClass: Option[String] = None,
  metricsParamsJsonPath: Option[String] = None,
  paramsPath: String = "params",
  engineInstanceId: Option[String] = None,
  mainClass: Option[String] = None)

case class CommonArgs(
  preDeployment: Boolean = false,
  batch: String = "",
  sparkPassThrough: Seq[String] = Seq(),
  driverPassThrough: Seq[String] = Seq(),
  pioHome: Option[String] = None,
  sparkHome: Option[String] = None,
  baseEngineURL: String = "",
  engineId: String = "",
  baseEngineId: String = "",
  engineParamsKey: Option[String] = None,
  evaluation: Option[String] = None,
  engineParamsGenerator: Option[String] = None,
  variantJson: File = new File("engine.json"),
  manifestJson: File = new File("manifest.json"),
  stopAfterRead: Boolean = false,
  stopAfterPrepare: Boolean = false,
  skipSanityCheck: Boolean = false,
  verbose: Boolean = false,
  verbosity: Int = 0,
  sparkKryo: Boolean = false,
  scratchUri: Option[URI] = None,
  jsonExtractor: JsonExtractorOption = JsonExtractorOption.Both)

case class BuildArgs(
  sbt: Option[File] = None,
  sbtExtra: Option[String] = None,
  sbtAssemblyPackageDependency: Boolean = true,
  sbtClean: Boolean = false,
  uberJar: Boolean = false,
  forceGeneratePIOSbt: Boolean = false)

case class DeployArgs(
  ip: String = "0.0.0.0",
  port: Int = 8000,
  logUrl: Option[String] = None,
  logPrefix: Option[String] = None)

case class EventServerArgs(
  enabled: Boolean = false,
  ip: String = "0.0.0.0",
  port: Int = 7070,
  stats: Boolean = false)

case class AdminServerArgs(
ip: String = "127.0.0.1",
port: Int = 7071)

case class DashboardArgs(
  ip: String = "127.0.0.1",
  port: Int = 9000)

case class UpgradeArgs(
  from: String = "0.0.0",
  to: String = "0.0.0",
  oldAppId: Int = 0,
  newAppId: Int = 0
)

case class RegisterArgs(
  deployPort: Int = -1
)

object Console extends Logging {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[ConsoleArgs]("pio") {
      override def showUsageOnError: Boolean = false
      head("PredictionIO Command Line Interface Console", BuildInfo.version)
      help("")
      note("Note that it is possible to supply pass-through arguments at\n" +
        "the end of the command by using a '--' separator, e.g.\n\n" +
        "pio train --params-path params -- --master spark://mycluster:7077\n" +
        "\nIn the example above, the '--master' argument will be passed to\n" +
        "underlying spark-submit command. Please refer to the usage section\n" +
        "for each command for more information.\n\n" +
        "The following options are common to all commands:\n")
      opt[String]("pio-home") action { (x, c) =>
        c.copy(common = c.common.copy(pioHome = Some(x)))
      } text("Root directory of a PredictionIO installation.\n" +
        "        Specify this if automatic discovery fail.")
      opt[String]("spark-home") action { (x, c) =>
        c.copy(common = c.common.copy(sparkHome = Some(x)))
      } text("Root directory of an Apache Spark installation.\n" +
        "        If not specified, will try to use the SPARK_HOME\n" +
        "        environmental variable. If this fails as well, default to\n" +
        "        current directory.")
      opt[String]("engine-id") abbr("ei") action { (x, c) =>
        c.copy(common = c.common.copy(engineId = x))
      } text("Specify an engine ID. Usually used by distributed deployment.")
      opt[String]("base-engine-url") abbr("be") action { (x, c) =>
        c.copy(common = c.common.copy(baseEngineURL = x))
      } text("specify the location of base engine")
      opt[String]("base-engine-id") action { (x, c) =>
        c.copy(common = c.common.copy(baseEngineId = x))
      }
      opt[File]("variant") abbr("v") action { (x, c) =>
        c.copy(common = c.common.copy(variantJson = x))
      }
      opt[File]("manifest") abbr("m") action { (x, c) =>
        c.copy(common = c.common.copy(manifestJson = x))
      }
      opt[File]("sbt") action { (x, c) =>
        c.copy(build = c.build.copy(sbt = Some(x)))
      } validate { x =>
        if (x.exists) {
          success
        } else {
          failure(s"${x.getCanonicalPath} does not exist.")
        }
      } text("Path to sbt. Default: sbt")
      opt[Unit]("verbose") action { (x, c) =>
        c.copy(common = c.common.copy(verbose = true))
      }
      opt[Unit]("spark-kryo") abbr("sk") action { (x, c) =>
        c.copy(common = c.common.copy(sparkKryo = true))
      }
      opt[String]("scratch-uri") action { (x, c) =>
        c.copy(common = c.common.copy(scratchUri = Some(new URI(x))))
      }
      note("")
      cmd("version").
        text("Displays the version of this command line console.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "version")
        }
      note("")
      cmd("help").action { (_, c) =>
        c.copy(commands = c.commands :+ "help")
      } children(
        arg[String]("<command>") optional()
          action { (x, c) =>
            c.copy(commands = c.commands :+ x)
          }
        )
      note("")
      cmd("build").
        text("Build an engine at the current directory.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "build")
        } children(
          opt[String]("sbt-extra") action { (x, c) =>
            c.copy(build = c.build.copy(sbtExtra = Some(x)))
          } text("Extra command to pass to SBT when it builds your engine."),
          opt[Unit]("clean") action { (x, c) =>
            c.copy(build = c.build.copy(sbtClean = true))
          } text("Clean build."),
          opt[Unit]("no-asm") action { (x, c) =>
            c.copy(build = c.build.copy(sbtAssemblyPackageDependency = false))
          } text("Skip building external dependencies assembly."),
          opt[Unit]("uber-jar") action { (x, c) =>
            c.copy(build = c.build.copy(uberJar = true))
          },
          opt[Unit]("generate-pio-sbt") action { (x, c) =>
            c.copy(build = c.build.copy(forceGeneratePIOSbt = true))
          }
        )
      note("")
      cmd("generate-security-key").
        text("generate a security to be stored in the db").
        action { (x, c) =>
          c.copy(commands = c.commands :+"generate-security-key")
        }
      note("")
      cmd("register").
        text("register an instance of the engine at the current directory").
        action { (x, c) =>
          c.copy(commands = c.commands :+"register")
        } children(
          opt[Int]("deploy-port") action { (x, c) =>
            c.copy(register = c.register.copy(deployPort = x))
          }
        )
      note("")
      cmd("unregister").
        text("Unregister an engine at the current directory.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "unregister")
        }
      note("")
      cmd("train").
        text("Kick off a training using an engine. This will produce an\n" +
          "engine instance. This command will pass all pass-through\n" +
          "arguments to its underlying spark-submit command.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "train")
        } children(
          opt[Unit]("pre-deployment") action { (x, c) =>
            c.copy(common = c.common.copy(preDeployment = true))
          } text("Batch label of the run."),
          opt[String]("batch") action { (x, c) =>
            c.copy(common = c.common.copy(batch = x))
          } text("Batch label of the run."),
          opt[String]("params-path") action { (x, c) =>
            c.copy(paramsPath = x)
          } text("Directory to lookup parameters JSON files. Default: params"),
          opt[String]("metrics-params") abbr("mp") action { (x, c) =>
            c.copy(metricsParamsJsonPath = Some(x))
          } text("Metrics parameters JSON file. Will try to use\n" +
            "        metrics.json in the base path."),
          opt[Unit]("skip-sanity-check") abbr("ssc") action { (x, c) =>
            c.copy(common = c.common.copy(skipSanityCheck = true))
          },
          opt[Unit]("stop-after-read") abbr("sar") action { (x, c) =>
            c.copy(common = c.common.copy(stopAfterRead = true))
          },
          opt[Unit]("stop-after-prepare") abbr("sap") action { (x, c) =>
            c.copy(common = c.common.copy(stopAfterPrepare = true))
          },
          opt[Unit]("uber-jar") action { (x, c) =>
            c.copy(build = c.build.copy(uberJar = true))
          },
          opt[Int]("verbosity") action { (x, c) =>
            c.copy(common = c.common.copy(verbosity = x))
          },
          opt[String]("engine-params-key") action { (x, c) =>
            c.copy(common = c.common.copy(engineParamsKey = Some(x)))
          },
          opt[String]("json-extractor") action { (x, c) =>
            c.copy(common = c.common.copy(jsonExtractor = JsonExtractorOption.withName(x)))
          } validate { x =>
              if (JsonExtractorOption.values.map(_.toString).contains(x)) {
                success
              } else {
                val validOptions = JsonExtractorOption.values.mkString("|")
                failure(s"$x is not a valid json-extractor option [$validOptions]")
              }
          }
        )
      note("")
      cmd("eval").
        text("Kick off an evaluation using an engine. This will produce an\n" +
          "engine instance. This command will pass all pass-through\n" +
          "arguments to its underlying spark-submit command.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "eval")
        } children(
          arg[String]("<evaluation-class>") action { (x, c) =>
            c.copy(common = c.common.copy(evaluation = Some(x)))
          },
          arg[String]("[<engine-parameters-generator-class>]") optional() action { (x, c) =>
            c.copy(common = c.common.copy(engineParamsGenerator = Some(x)))
          } text("Optional engine parameters generator class, overriding the first argument"),
          opt[String]("batch") action { (x, c) =>
            c.copy(common = c.common.copy(batch = x))
          } text("Batch label of the run."),
          opt[String]("json-extractor") action { (x, c) =>
            c.copy(common = c.common.copy(jsonExtractor = JsonExtractorOption.withName(x)))
          } validate { x =>
            if (JsonExtractorOption.values.map(_.toString).contains(x)) {
              success
            } else {
              val validOptions = JsonExtractorOption.values.mkString("|")
              failure(s"$x is not a valid json-extractor option [$validOptions]")
            }
          }
        )      
      note("")
      cmd("deploy").
        text("Deploy an engine instance as a prediction server. This\n" +
          "command will pass all pass-through arguments to its underlying\n" +
          "spark-submit command.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "deploy")
        } children(
          opt[String]("batch") action { (x, c) =>
            c.copy(common = c.common.copy(batch = x))
          } text("Batch label of the deployment."),
          opt[String]("engine-instance-id") action { (x, c) =>
            c.copy(engineInstanceId = Some(x))
          } text("Engine instance ID."),
          opt[String]("ip") action { (x, c) =>
            c.copy(deploy = c.deploy.copy(ip = x))
          },
          opt[Int]("port") action { (x, c) =>
            c.copy(deploy = c.deploy.copy(port = x))
          } text("Port to bind to. Default: 8000"),
          opt[Unit]("feedback") action { (_, c) =>
            c.copy(eventServer = c.eventServer.copy(enabled = true))
          } text("Enable feedback loop to event server."),
          opt[String]("event-server-ip") action { (x, c) =>
            c.copy(eventServer = c.eventServer.copy(ip = x))
          },
          opt[Int]("event-server-port") action { (x, c) =>
            c.copy(eventServer = c.eventServer.copy(port = x))
          } text("Event server port. Default: 7070"),
          opt[Int]("admin-server-port") action { (x, c) =>
            c.copy(adminServer = c.adminServer.copy(port = x))
          } text("Admin server port. Default: 7071"),
          opt[String]("admin-server-ip") action { (x, c) =>
          c.copy(adminServer = c.adminServer.copy(ip = x))
          } text("Admin server IP. Default: localhost"),
          opt[String]("accesskey") action { (x, c) =>
            c.copy(accessKey = c.accessKey.copy(accessKey = x))
          } text("Access key of the App where feedback data will be stored."),
          opt[Unit]("uber-jar") action { (x, c) =>
            c.copy(build = c.build.copy(uberJar = true))
          },
          opt[String]("log-url") action { (x, c) =>
            c.copy(deploy = c.deploy.copy(logUrl = Some(x)))
          },
          opt[String]("log-prefix") action { (x, c) =>
            c.copy(deploy = c.deploy.copy(logPrefix = Some(x)))
          },
          opt[String]("json-extractor") action { (x, c) =>
            c.copy(common = c.common.copy(jsonExtractor = JsonExtractorOption.withName(x)))
          } validate { x =>
            if (JsonExtractorOption.values.map(_.toString).contains(x)) {
              success
            } else {
              val validOptions = JsonExtractorOption.values.mkString("|")
              failure(s"$x is not a valid json-extractor option [$validOptions]")
            }
          }
        )
      note("")
      cmd("undeploy").
        text("Undeploy an engine instance as a prediction server.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "undeploy")
        } children(
          opt[String]("ip") action { (x, c) =>
            c.copy(deploy = c.deploy.copy(ip = x))
          },
          opt[Int]("port") action { (x, c) =>
            c.copy(deploy = c.deploy.copy(port = x))
          } text("Port to unbind from. Default: 8000")
        )
      note("")
      cmd("dashboard").
        text("Launch a dashboard at the specific IP and port.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "dashboard")
        } children(
          opt[String]("ip") action { (x, c) =>
            c.copy(dashboard = c.dashboard.copy(ip = x))
          },
          opt[Int]("port") action { (x, c) =>
            c.copy(dashboard = c.dashboard.copy(port = x))
          } text("Port to bind to. Default: 9000")
        )
      note("")
      cmd("eventserver").
        text("Launch an Event Server at the specific IP and port.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "eventserver")
        } children(
          opt[String]("ip") action { (x, c) =>
            c.copy(eventServer = c.eventServer.copy(ip = x))
          },
          opt[Int]("port") action { (x, c) =>
            c.copy(eventServer = c.eventServer.copy(port = x))
          } text("Port to bind to. Default: 7070"),
          opt[Unit]("stats") action { (x, c) =>
            c.copy(eventServer = c.eventServer.copy(stats = true))
          }
        )
      cmd("adminserver").
        text("Launch an Admin Server at the specific IP and port.").
        action { (_, c) =>
        c.copy(commands = c.commands :+ "adminserver")
      } children(
        opt[String]("ip") action { (x, c) =>
          c.copy(adminServer = c.adminServer.copy(ip = x))
        } text("IP to bind to. Default: localhost"),
        opt[Int]("port") action { (x, c) =>
          c.copy(adminServer = c.adminServer.copy(port = x))
        } text("Port to bind to. Default: 7071")
        )
      note("")
      cmd("run").
        text("Launch a driver program. This command will pass all\n" +
          "pass-through arguments to its underlying spark-submit command.\n" +
          "In addition, it also supports a second level of pass-through\n" +
          "arguments to the driver program, e.g.\n" +
          "pio run -- --master spark://localhost:7077 -- --driver-arg foo").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "run")
        } children(
          arg[String]("<main class>") action { (x, c) =>
            c.copy(mainClass = Some(x))
          } text("Main class name of the driver program."),
          opt[String]("sbt-extra") action { (x, c) =>
            c.copy(build = c.build.copy(sbtExtra = Some(x)))
          } text("Extra command to pass to SBT when it builds your engine."),
          opt[Unit]("clean") action { (x, c) =>
            c.copy(build = c.build.copy(sbtClean = true))
          } text("Clean build."),
          opt[Unit]("no-asm") action { (x, c) =>
            c.copy(build = c.build.copy(sbtAssemblyPackageDependency = false))
          } text("Skip building external dependencies assembly.")
        )
      note("")
      cmd("status").
        text("Displays status information about the PredictionIO system.").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "status")
        }
      note("")
      cmd("upgrade").
        text("Upgrade tool").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "upgrade")
        } children(
          arg[String]("<from version>") action { (x, c) =>
            c.copy(upgrade = c.upgrade.copy(from = x))
          } text("The version upgraded from."),
          arg[String]("<to version>") action { (x, c) =>
            c.copy(upgrade = c.upgrade.copy(to = x))
          } text("The version upgraded to."),
          arg[Int]("<old App ID>") action { (x, c) =>
            c.copy(upgrade = c.upgrade.copy(oldAppId = x))
          } text("Old App ID."),
          arg[Int]("<new App ID>") action { (x, c) =>
            c.copy(upgrade = c.upgrade.copy(newAppId = x))
          } text("New App ID.")
        )
      note("")
      cmd("app").
        text("Manage apps.\n").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "app")
        } children(
          cmd("new").
            text("Create a new app key to app ID mapping.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "new")
            } children(
              opt[Int]("id") action { (x, c) =>
                c.copy(app = c.app.copy(id = Some(x)))
              },
              opt[String]("description") action { (x, c) =>
                c.copy(app = c.app.copy(description = Some(x)))
              },
              opt[String]("access-key") action { (x, c) =>
                c.copy(accessKey = c.accessKey.copy(accessKey = x))
              },
              arg[String]("<name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              }
            ),
          note(""),
          cmd("list").
            text("List all apps.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "list")
            },
          note(""),
          cmd("show").
            text("Show details of an app.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "show")
            } children (
              arg[String]("<name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              } text("Name of the app to be shown.")
            ),
          note(""),
          cmd("delete").
            text("Delete an app.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "delete")
            } children(
              arg[String]("<name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              } text("Name of the app to be deleted."),
              opt[Unit]("force") abbr("f") action { (x, c) =>
                c.copy(app = c.app.copy(force = true))
              } text("Delete an app without prompting for confirmation")
            ),
          note(""),
          cmd("data-delete").
            text("Delete data of an app").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "data-delete")
            } children(
              arg[String]("<name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              } text("Name of the app whose data to be deleted."),
              opt[String]("channel") action { (x, c) =>
                c.copy(app = c.app.copy(dataDeleteChannel = Some(x)))
              } text("Name of channel whose data to be deleted."),
              opt[Unit]("all") action { (x, c) =>
                c.copy(app = c.app.copy(all = true))
              } text("Delete data of all channels including default"),
              opt[Unit]("force") abbr("f") action { (x, c) =>
                c.copy(app = c.app.copy(force = true))
              } text("Delete data of an app without prompting for confirmation")
            ),
          note(""),
          cmd("channel-new").
            text("Create a new channel for the app.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "channel-new")
            } children (
              arg[String]("<name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              } text("App name."),
              arg[String]("<channel>") action { (x, c) =>
                c.copy(app = c.app.copy(channel = x))
              } text ("Channel name to be created.")
            ),
          note(""),
          cmd("channel-delete").
            text("Delete a channel of the app.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "channel-delete")
            } children (
              arg[String]("<name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              } text("App name."),
              arg[String]("<channel>") action { (x, c) =>
                c.copy(app = c.app.copy(channel = x))
              } text ("Channel name to be deleted."),
              opt[Unit]("force") abbr("f") action { (x, c) =>
                c.copy(app = c.app.copy(force = true))
              } text("Delete a channel of the app without prompting for confirmation")
            )
        )
      note("")
      cmd("accesskey").
        text("Manage app access keys.\n").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "accesskey")
        } children(
          cmd("new").
            text("Add allowed event(s) to an access key.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "new")
            } children(
              opt[String]("key") action { (x, c) =>
                c.copy(accessKey = c.accessKey.copy(accessKey = x))
              },
              arg[String]("<app name>") action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              },
              arg[String]("[<event1> <event2> ...]") unbounded() optional()
                action { (x, c) =>
                  c.copy(accessKey = c.accessKey.copy(
                    events = c.accessKey.events :+ x))
                }
            ),
          cmd("list").
            text("List all access keys of an app.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "list")
            } children(
              arg[String]("<app name>") optional() action { (x, c) =>
                c.copy(app = c.app.copy(name = x))
              } text("App name.")
            ),
          note(""),
          cmd("delete").
            text("Delete an access key.").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "delete")
            } children(
              arg[String]("<access key>") action { (x, c) =>
                c.copy(accessKey = c.accessKey.copy(accessKey = x))
              } text("The access key to be deleted.")
            )
        )
      cmd("template").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "template")
        } children(
          cmd("get").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "get")
            } children(
              arg[String]("<template ID>") required() action { (x, c) =>
                c.copy(template = c.template.copy(repository = x))
              },
              arg[String]("<new engine directory>") action { (x, c) =>
                c.copy(template = c.template.copy(directory = x))
              },
              opt[String]("version") action { (x, c) =>
                c.copy(template = c.template.copy(version = Some(x)))
              },
              opt[String]("name") action { (x, c) =>
                c.copy(template = c.template.copy(name = Some(x)))
              },
              opt[String]("package") action { (x, c) =>
                c.copy(template = c.template.copy(packageName = Some(x)))
              },
              opt[String]("email") action { (x, c) =>
                c.copy(template = c.template.copy(email = Some(x)))
              }
            ),
          cmd("list").
            action { (_, c) =>
              c.copy(commands = c.commands :+ "list")
            }
        )
      cmd("export").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "export")
        } children(
          opt[Int]("appid") required() action { (x, c) =>
            c.copy(export = c.export.copy(appId = x))
          },
          opt[String]("output") required() action { (x, c) =>
            c.copy(export = c.export.copy(outputPath = x))
          },
          opt[String]("format") action { (x, c) =>
            c.copy(export = c.export.copy(format = x))
          },
          opt[String]("channel") action { (x, c) =>
            c.copy(export = c.export.copy(channel = Some(x)))
          }
        )
      cmd("import").
        action { (_, c) =>
          c.copy(commands = c.commands :+ "import")
        } children(
          opt[Int]("appid") required() action { (x, c) =>
            c.copy(imprt = c.imprt.copy(appId = x))
          },
          opt[String]("input") required() action { (x, c) =>
            c.copy(imprt = c.imprt.copy(inputPath = x))
          },
          opt[String]("channel") action { (x, c) =>
            c.copy(imprt = c.imprt.copy(channel = Some(x)))
          }
        )
    }

    val separatorIndex = args.indexWhere(_ == "--")
    val (consoleArgs, theRest) =
      if (separatorIndex == -1) {
        (args, Array[String]())
      } else {
        args.splitAt(separatorIndex)
      }
    val allPassThroughArgs = theRest.drop(1)
    val secondSepIdx = allPassThroughArgs.indexWhere(_ == "--")
    val (sparkPassThroughArgs, driverPassThroughArgs) =
      if (secondSepIdx == -1) {
        (allPassThroughArgs, Array[String]())
      } else {
        val t = allPassThroughArgs.splitAt(secondSepIdx)
        (t._1, t._2.drop(1))
      }

    parser.parse(consoleArgs, ConsoleArgs()) map { pca =>
      val ca = pca.copy(common = pca.common.copy(
        sparkPassThrough = sparkPassThroughArgs,
        driverPassThrough = driverPassThroughArgs))
      WorkflowUtils.modifyLogging(ca.common.verbose)
      val rv: Int = ca.commands match {
        case Seq("") =>
          System.err.println(help())
          1
        case Seq("version") =>
          version(ca)
          0
        case Seq("build") =>
          regenerateManifestJson(ca.common.manifestJson)
          build(ca)
        case Seq("generate-security-key") =>
          generateSecuritykey()
          0
        case Seq("register") =>
          val eId = ca.common.engineId
          registerEngineWithArgs(ca, eId)
          0
        case Seq("unregister") =>
          unregister(ca)
          0
        case Seq("train") =>
          //regenerateManifestJson(ca.common.manifestJson)
          train(ca)
        case Seq("eval") =>
          regenerateManifestJson(ca.common.manifestJson)
          train(ca)
        case Seq("deploy") =>
          deploy(ca)
        case Seq("undeploy") =>
          undeploy(ca)
        case Seq("dashboard") =>
          dashboard(ca)
          0
        case Seq("eventserver") =>
          eventserver(ca)
          0
        case Seq("adminserver") =>
          adminserver(ca)
          0
        case Seq("run") =>
          generateManifestJson(ca.common.manifestJson)
          run(ca)
        case Seq("status") =>
          status(ca)
        case Seq("upgrade") =>
          upgrade(ca)
          0
        case Seq("app", "new") =>
          App.create(ca)
        case Seq("app", "list") =>
          App.list(ca)
        case Seq("app", "show") =>
          App.show(ca)
        case Seq("app", "delete") =>
          App.delete(ca)
        case Seq("app", "data-delete") =>
          App.dataDelete(ca)
        case Seq("app", "channel-new") =>
          App.channelNew(ca)
        case Seq("app", "channel-delete") =>
          App.channelDelete(ca)
        case Seq("accesskey", "new") =>
          AccessKey.create(ca)
        case Seq("accesskey", "list") =>
          AccessKey.list(ca)
        case Seq("accesskey", "delete") =>
          AccessKey.delete(ca)
        case Seq("template", "get") =>
          Template.get(ca)
        case Seq("template", "list") =>
          Template.list(ca)
        case Seq("export") =>
          Export.eventsToFile(ca)
        case Seq("import") =>
          Import.fileToEvents(ca)
        case _ =>
          System.err.println(help(ca.commands))
          1
      }
      sys.exit(rv)
    } getOrElse {
      val command = args.toSeq.filterNot(_.startsWith("--")).head
      System.err.println(help(Seq(command)))
      sys.exit(1)
    }
  }

  def help(commands: Seq[String] = Seq()): String = {
    if (commands.isEmpty) {
      mainHelp
    } else {
      val stripped =
        (if (commands.head == "help") commands.drop(1) else commands).
          mkString("-")
      helpText.getOrElse(stripped, s"Help is unavailable for ${stripped}.")
    }
  }

  val mainHelp = txt.main().toString

  val helpText = Map(
    "" -> mainHelp,
    "status" -> txt.status().toString,
    "upgrade" -> txt.upgrade().toString,
    "version" -> txt.version().toString,
    "template" -> txt.template().toString,
    "build" -> txt.build().toString,
    "train" -> txt.train().toString,
    "deploy" -> txt.deploy().toString,
    "eventserver" -> txt.eventserver().toString,
    "adminserver" -> txt.adminserver().toString,
    "app" -> txt.app().toString,
    "accesskey" -> txt.accesskey().toString,
    "import" -> txt.imprt().toString,
    "export" -> txt.export().toString,
    "run" -> txt.run().toString,
    "eval" -> txt.eval().toString,
    "dashboard" -> txt.dashboard().toString)

  def version(ca: ConsoleArgs): Unit = println(BuildInfo.version)

  def registerEngineWithArgs(ca: ConsoleArgs, engineId: String): Int = {
    if (engineId == ""){
      error("No engineId specified. Abort engine register")
      return 1
    }
    val jarFiles = jarFilesForScalaWithBaseEngineURL(ca.common.baseEngineURL)
    if (jarFiles.isEmpty) {
      error("No engine found. Your build might have failed. Aborting.")
      return 1
    }
    jarFiles foreach { f => info(s"Found ${f.getName}")}
    val manifest = EngineManifest(
      id = engineId,
      version = engineId,
      name = new File(ca.common.baseEngineURL).getName,
      description = Some(manifestManualgenTag),
      files = Seq(),
      engineFactory = "",
      port = ca.register.deployPort,
      trainingStatus = "NONE",
      baseEngine = ca.common.baseEngineId)
    RegisterEngine.registerEngineWithManifest(manifest, jarFiles)
    info("Your engine is ready for training.")
    0
  }

  def build(ca: ConsoleArgs): Int = {
    Template.verifyTemplateMinVersion(new File("template.json"))
    compile(ca)
    info("Looking for an engine...")
    val jarFiles = jarFilesForScala
    if (jarFiles.isEmpty) {
      error("No engine found. Your build might have failed. Aborting.")
      return 1
    }
    jarFiles foreach { f => info(s"Found ${f.getName}")}
    // RegisterEngine.registerEngine(
    //   ca.common.manifestJson,
    //   jarFiles,
    //   false)
    info("Your engine is built, still need to run pio register --engine-id{} --variant{} to make engine deployable.")
    0
  }

  def generateSecuritykey(): Unit = {
    val key = getSecureString()
    val serverConfigs = storage.Storage.getMetaDataServerConfigs
    val svConfig = new ServerConfig(securityKey = key)
    serverConfigs.insert(svConfig)
    info("Created new security-key:")
            info(s"  key: ${key}")
  }

  def getSecureString():String = {
    val sr = SecureRandom.getInstance("SHA1PRNG")
    val srBytes = Array.fill(48)(0.toByte)
    sr.nextBytes(srBytes)
    Base64.encodeBase64URLSafeString(srBytes)
  }

  def unregister(ca: ConsoleArgs): Unit = {
    RegisterEngine.unregisterEngine(ca.common.manifestJson)
  }

  def train(ca: ConsoleArgs): Int = {
    Template.verifyTemplateMinVersion(new File("template.json"))
    withRegisteredManifest(
      ca.common.engineId,
      ca.common.engineId) { em =>
      RunWorkflow.newRunWorkflow(ca, em)
    }
  }

  def deploy(ca: ConsoleArgs): Int = {
    Template.verifyTemplateMinVersion(new File("template.json"))
    withRegisteredManifest(
      ca.common.engineId,
      ca.common.engineId) { em =>
      val variantJson = parse(Source.fromFile(ca.common.variantJson).mkString)
      val variantId = variantJson \ "id" match {
        case JString(s) => s
        case _ =>
          error("Unable to read engine variant ID from " +
            s"${ca.common.variantJson.getCanonicalPath}. Aborting.")
          return 1
      }
      val engineInstances = storage.Storage.getMetaDataEngineInstances
      val engineInstance = ca.engineInstanceId map { eid =>
        engineInstances.get(eid)
      } getOrElse {
        engineInstances.getLatestCompleted(em.id, em.version, variantId)
      }
      engineInstance map { r =>
        RunServer.newRunServer(ca, em, r.id)
        //RunServer.newSingleQuery(ca,em,r.id)
      } getOrElse {
        ca.engineInstanceId map { eid =>
          error(
            s"Invalid engine instance ID ${ca.engineInstanceId}. Aborting.")
        } getOrElse {
          error(
            s"No valid engine instance found for engine ${em.id} " +
              s"${em.version}.\nTry running 'train' before 'deploy'. Aborting.")
        }
        1
      }
    }
  }

  def dashboard(ca: ConsoleArgs): Unit = {
    info(s"Creating dashboard at ${ca.dashboard.ip}:${ca.dashboard.port}")
    Dashboard.createDashboard(DashboardConfig(
      ip = ca.dashboard.ip,
      port = ca.dashboard.port))
  }

  def eventserver(ca: ConsoleArgs): Unit = {
    info(
      s"Creating Event Server at ${ca.eventServer.ip}:${ca.eventServer.port}")
    ConsoleEventServer.createEventServer(ConsoleEventServerConfig(
      ip = ca.eventServer.ip,
      port = ca.eventServer.port,
      stats = ca.eventServer.stats))
  }

  def adminserver(ca: ConsoleArgs): Unit = {
    info(
      s"Creating Admin Server at ${ca.adminServer.ip}:${ca.adminServer.port}")
    AdminServer.createAdminServer(AdminServerConfig(
      ip = ca.adminServer.ip,
      port = ca.adminServer.port
    ))
  }

  def undeploy(ca: ConsoleArgs): Int = {
    val serverUrl = s"http://${ca.deploy.ip}:${ca.deploy.port}"
    info(
      s"Undeploying any existing engine instance at ${serverUrl}")
    try {
      val code = Http(s"${serverUrl}/stop").asString.code
      code match {
        case 200 => 0
        case 404 =>
          error(s"Another process is using ${serverUrl}. Unable to undeploy.")
          1
        case _ =>
          error(s"Another process is using ${serverUrl}, or an existing " +
            s"engine server is not responding properly (HTTP ${code}). " +
            "Unable to undeploy.")
            1
      }
    } catch {
      case e: java.net.ConnectException =>
        warn(s"Nothing at ${serverUrl}")
        0
      case _: Throwable =>
        error("Another process might be occupying " +
          s"${ca.deploy.ip}:${ca.deploy.port}. Unable to undeploy.")
        1
    }
  }

  def compile(ca: ConsoleArgs): Unit = {
    // only add pioVersion to sbt if project/pio.sbt exists
    if (new File("project", "pio-build.sbt").exists || ca.build.forceGeneratePIOSbt) {
      FileUtils.writeLines(
        new File("pio.sbt"),
        Seq(
          "// Generated automatically by pio build.",
          "// Changes in this file will be overridden.",
          "",
          "pioVersion := \"" + BuildInfo.version + "\""))
    }
    implicit val formats = Utils.json4sDefaultFormats

    val sbt = detectSbt(ca)
    info(s"Using command '${sbt}' at the current working directory to build.")
    info("If the path above is incorrect, this process will fail.")
    val asm =
      if (ca.build.sbtAssemblyPackageDependency) {
        " assemblyPackageDependency"
      } else {
        ""
      }
    val clean = if (ca.build.sbtClean) " clean" else ""
    val buildCmd = s"${sbt} ${ca.build.sbtExtra.getOrElse("")}${clean} " +
      (if (ca.build.uberJar) "assembly" else s"package${asm}")
    val core = new File(s"pio-assembly-${BuildInfo.version}.jar")
    if (ca.build.uberJar) {
      info(s"Uber JAR enabled. Putting ${core.getName} in lib.")
      val dst = new File("lib")
      dst.mkdir()
      FileUtils.copyFileToDirectory(
        coreAssembly(ca.common.pioHome.get),
        dst,
        true)
    } else {
      if (new File("engine.json").exists()) {
        info(s"Uber JAR disabled. Making sure lib/${core.getName} is absent.")
        new File("lib", core.getName).delete()
      } else {
        info("Uber JAR disabled, but current working directory does not look " +
          s"like an engine project directory. Please delete lib/${core.getName} manually.")
      }
    }
    info(s"Going to run: ${buildCmd}")
    try {
      val r =
        if (ca.common.verbose) {
          buildCmd.!(ProcessLogger(line => info(line), line => error(line)))
        } else {
          buildCmd.!(ProcessLogger(
            line => outputSbtError(line),
            line => outputSbtError(line)))
        }
      if (r != 0) {
        error(s"Return code of previous step is ${r}. Aborting.")
        sys.exit(1)
      }
      info("Build finished successfully.")
    } catch {
      case e: java.io.IOException =>
        error(s"${e.getMessage}")
        sys.exit(1)
    }
  }

  private def outputSbtError(line: String): Unit = {
    """\[.*error.*\]""".r findFirstIn line foreach { _ => error(line) }
  }

  def run(ca: ConsoleArgs): Int = {
    compile(ca)

    val extraFiles = WorkflowUtils.thirdPartyConfFiles

    val jarFiles = jarFilesForScala
    jarFiles foreach { f => info(s"Found JAR: ${f.getName}") }
    val allJarFiles = jarFiles.map(_.getCanonicalPath)
    val cmd = s"${getSparkHome(ca.common.sparkHome)}/bin/spark-submit --jars " +
      s"${allJarFiles.mkString(",")} " +
      (if (extraFiles.size > 0) {
        s"--files ${extraFiles.mkString(",")} "
      } else {
        ""
      }) +
      "--class " +
      s"${ca.mainClass.get} ${ca.common.sparkPassThrough.mkString(" ")} " +
      coreAssembly(ca.common.pioHome.get) + " " +
      ca.common.driverPassThrough.mkString(" ")
    val proc = Process(
      cmd,
      None,
      "SPARK_YARN_USER_ENV" -> sys.env.filter(kv => kv._1.startsWith("PIO_")).
        map(kv => s"${kv._1}=${kv._2}").mkString(","))
    info(s"Submission command: ${cmd}")
    val r = proc.!
    if (r != 0) {
      error(s"Return code of previous step is ${r}. Aborting.")
      return 1
    }
    r
  }

  def status(ca: ConsoleArgs): Int = {
    info("Inspecting PredictionIO...")
    ca.common.pioHome map { pioHome =>
      info(s"PredictionIO ${BuildInfo.version} is installed at $pioHome")
    } getOrElse {
      error("Unable to locate PredictionIO installation. Aborting.")
      return 1
    }
    info("Inspecting Apache Spark...")
    val sparkHome = getSparkHome(ca.common.sparkHome)
    if (new File(s"$sparkHome/bin/spark-submit").exists) {
      info(s"Apache Spark is installed at $sparkHome")
      val sparkMinVersion = "1.3.0"
      val sparkReleaseFile = new File(s"$sparkHome/RELEASE")
      if (sparkReleaseFile.exists) {
        val sparkReleaseStrings =
          Source.fromFile(sparkReleaseFile).mkString.split(' ')
        if (sparkReleaseStrings.length < 2) {
          warn(stripMarginAndNewlines(
            s"""|Apache Spark version information cannot be found (RELEASE file
                |is empty). This is a known issue for certain vendors (e.g.
                |Cloudera). Please make sure you are using a version of at least
                |$sparkMinVersion."""))
        } else {
          val sparkReleaseVersion = sparkReleaseStrings(1)
          val parsedMinVersion = Version.apply(sparkMinVersion)
          val parsedCurrentVersion = Version.apply(sparkReleaseVersion)
          if (parsedCurrentVersion >= parsedMinVersion) {
            info(stripMarginAndNewlines(
              s"""|Apache Spark $sparkReleaseVersion detected (meets minimum
                  |requirement of $sparkMinVersion)"""))
          } else {
            error(stripMarginAndNewlines(
              s"""|Apache Spark $sparkReleaseVersion detected (does not meet
                  |minimum requirement. Aborting."""))
          }
        }
      } else {
        warn(stripMarginAndNewlines(
          s"""|Apache Spark version information cannot be found. If you are
              |using a developmental tree, please make sure you are using a
              |version of at least $sparkMinVersion."""))
      }
    } else {
      error("Unable to locate a proper Apache Spark installation. Aborting.")
      return 1
    }
    info("Inspecting storage backend connections...")
    try {
      storage.Storage.verifyAllDataObjects()
    } catch {
      case e: Throwable =>
        error("Unable to connect to all storage backends successfully. The " +
          "following shows the error message from the storage backend.")
        error(s"${e.getMessage} (${e.getClass.getName})", e)
        error("Dumping configuration of initialized storage backend sources. " +
          "Please make sure they are correct.")
        storage.Storage.config.get("sources") map { src =>
          src foreach { case (s, p) =>
            error(s"Source Name: $s; Type: ${p.getOrElse("type", "(error)")}; " +
              s"Configuration: ${p.getOrElse("config", "(error)")}")
          }
        } getOrElse {
          error("No properly configured storage backend sources.")
        }
        return 1
    }
    info("(sleeping 5 seconds for all messages to show up...)")
    Thread.sleep(5000)
    info("Your system is all ready to go.")
    0
  }

  def upgrade(ca: ConsoleArgs): Unit = {
    (ca.upgrade.from, ca.upgrade.to) match {
      case ("0.8.2", "0.8.3") => {
        Upgrade_0_8_3.runMain(ca.upgrade.oldAppId, ca.upgrade.newAppId)
      }
      case _ =>
        println(s"Upgrade from version ${ca.upgrade.from} to ${ca.upgrade.to}"
          + s" is not supported.")
    }
  }

  def coreAssembly(pioHome: String): File = {
    val core = s"pio-assembly-${BuildInfo.version}.jar"
    val coreDir =
      if (new File(pioHome + File.separator + "RELEASE").exists) {
        new File(pioHome + File.separator + "lib")
      } else {
        new File(pioHome + File.separator + "assembly")
      }
    val coreFile = new File(coreDir, core)
    if (coreFile.exists) {
      coreFile
    } else {
      error(s"PredictionIO Core Assembly (${coreFile.getCanonicalPath}) does " +
        "not exist. Aborting.")
      sys.exit(1)
    }
  }

  val manifestAutogenTag = "pio-autogen-manifest"
  val manifestManualgenTag = "pio-custom-manifest"

  def regenerateManifestJson(json: File): Unit = {
    val cwd = sys.props("user.dir")
    val ha = java.security.MessageDigest.getInstance("SHA-1").
      digest(cwd.getBytes).map("%02x".format(_)).mkString
    if (json.exists) {
      val em = readManifestJson(json)
      if (em.description == Some(manifestAutogenTag) && ha != em.version) {
        warn("This engine project directory contains an auto-generated " +
          "manifest that has been copied/moved from another location. ")
        warn("Regenerating the manifest to reflect the updated location. " +
          "This will dissociate with all previous engine instances.")
        generateManifestJson(json)
      } else {
        info(s"Using existing engine manifest JSON at ${json.getCanonicalPath}")
      }
    } else {
      generateManifestJson(json)
    }
  }

  def generateManifestJson(json: File): Unit = {
    val cwd = sys.props("user.dir")
    implicit val formats = Utils.json4sDefaultFormats +
      new EngineManifestSerializer
    val rand = Random.alphanumeric.take(32).mkString
    val ha = java.security.MessageDigest.getInstance("SHA-1").
      digest(cwd.getBytes).map("%02x".format(_)).mkString
    val em = EngineManifest(
      id = rand,
      version = ha,
      name = new File(cwd).getName,
      description = Some(manifestAutogenTag),
      files = Seq(),
      engineFactory = "",
      port = -1,
      trainingStatus = "NONE",
      baseEngine = "")
    try {
      FileUtils.writeStringToFile(json, write(em), "ISO-8859-1")
    } catch {
      case e: java.io.IOException =>
        error(s"Cannot generate ${json} automatically (${e.getMessage}). " +
          "Aborting.")
        sys.exit(1)
    }
  }

  def readManifestJson(json: File): EngineManifest = {
    implicit val formats = Utils.json4sDefaultFormats +
      new EngineManifestSerializer
    try {
      read[EngineManifest](Source.fromFile(json).mkString)
    } catch {
      case e: java.io.FileNotFoundException =>
        error(s"${json.getCanonicalPath} does not exist. Aborting.")
        sys.exit(1)
      case e: MappingException =>
        error(s"${json.getCanonicalPath} has invalid content: " +
          e.getMessage)
        sys.exit(1)
    }
  }

  def withRegisteredManifest(
      engineId: String,
      engineVersion: String)(
      op: EngineManifest => Int): Int = {
    storage.Storage.getMetaDataEngineManifests.get(engineId, engineVersion) map {
      op
    } getOrElse {
      error(s"Engine ${engineId} ${engineVersion} cannot be found in the system.")
      error("Possible reasons:")
      error("- incorrect engineId")
      error("- the meta data store is offline.")
      1
    }
  }

  def withCustomRegisteredManifest(
      engineId: Option[String],
      engineVersion: Option[String])(
      op: EngineManifest => Int): Int = {
    val id = engineId getOrElse ""
    val version = engineVersion getOrElse ""
    storage.Storage.getMetaDataEngineManifests.get(id, version) map {
      op
    } getOrElse {
      error(s"Engine ${id} ${version} cannot be found in the system.")
      error("Possible reasons:")
      error("- the engine is not yet built by the 'build' command;")
      error("- the meta data store is offline.")
      1
    }
  }

  def getCustomRegisteredManifestList(engineIdList: String): List[EngineManifest] = {
    val idList = engineIdList.split("-")
    var manifests = new ListBuffer[EngineManifest]()
    for (id <- idList){
      storage.Storage.getMetaDataEngineManifests.get(id, id) map { manifest =>
          manifests += manifest
        } getOrElse {
        error(s"Engine ${id} cannot be found in the system.")
        error("Possible reasons:")
        error("- the engine is not yet built by the 'build' command;")
        error("- the meta data store is offline.")
      }
    }
    manifests.toList
  }

  def jarFilesAt(path: File): Array[File] = recursiveListFiles(path) filter {
    _.getName.toLowerCase.endsWith(".jar")
  }

  def jarFilesForScalaWithBaseEngineURL(baseEngineURL: String): Array[File] = {
    val libFiles = jarFilesForScalaFilter(jarFilesAt(new File(s"${baseEngineURL}/lib")))
    val targetFiles = jarFilesForScalaFilter(jarFilesAt(new File(s"${baseEngineURL}/target/scala-${scalaVersionNoPatch}")))
    if(targetFiles.size > 0) targetFiles else libFiles
  }

  def jarFilesForScala: Array[File] = {
    val libFiles = jarFilesForScalaFilter(jarFilesAt(new File("lib")))
    val targetFiles = jarFilesForScalaFilter(jarFilesAt(new File("target" +
      File.separator + s"scala-${scalaVersionNoPatch}")))
    // Use libFiles is target is empty.
    if (targetFiles.size > 0) targetFiles else libFiles
  }

  def jarFilesForScalaFilter(jars: Array[File]): Array[File] =
    jars.filterNot { f =>
      f.getName.toLowerCase.endsWith("-javadoc.jar") ||
      f.getName.toLowerCase.endsWith("-sources.jar")
    }

  def recursiveListFiles(f: File): Array[File] = {
    Option(f.listFiles) map { these =>
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    } getOrElse Array[File]()
  }

  def getSparkHome(sparkHome: Option[String]): String = {
    sparkHome getOrElse {
      sys.env.getOrElse("SPARK_HOME", ".")
    }
  }

  def versionNoPatch(fullVersion: String): String = {
    val v = """^(\d+\.\d+)""".r
    val versionNoPatch = for {
      v(np) <- v findFirstIn fullVersion
    } yield np
    versionNoPatch.getOrElse(fullVersion)
  }

  def scalaVersionNoPatch: String = versionNoPatch(BuildInfo.scalaVersion)

  def detectSbt(ca: ConsoleArgs): String = {
    ca.build.sbt map {
      _.getCanonicalPath
    } getOrElse {
      val f = new File(Seq(ca.common.pioHome.get, "sbt", "sbt").mkString(
        File.separator))
      if (f.exists) f.getCanonicalPath else "sbt"
    }
  }

  def stripMarginAndNewlines(string: String): String =
    string.stripMargin.replaceAll("\n", " ")


  // Event server code starts here
  class  ConsoleEventServiceActor(
      val eventClient: LEvents,
      val accessKeysClient: AccessKeys,
      val channelsClient: Channels,
      val config: ConsoleEventServerConfig) extends HttpServiceActor {

    // reference the Console class
    val console = Console.this

    object Json4sProtocol extends Json4sSupport {
      implicit def json4sFormats: Formats = DefaultFormats +
        new EventJson4sSupport.APISerializer +
        new BatchEventsJson4sSupport.APISerializer +
        // NOTE: don't use Json4s JodaTimeSerializers since it has issues,
        // some format not converted, or timezone not correct
        new DateTimeJson4sSupport.Serializer
    }

    val pio_root = sys.env("PIO_ROOT")
    //val pio_root = "/$HOME/PredictionIO/LFPredictionIO/apache-predictionio-0.10.0-incubating"

    val MaxNumberOfEventsPerBatchRequest = 50

    val logger = Logging(context.system, this)

    var trainingQueue: Queue[EngineData] = Queue()
    var trainingLeft = 2;

    val internalAccessKey = generateAccessKey()
    // we use the enclosing ActorContext's or ActorSystem's dispatcher for our
    // Futures
    implicit def executionContext: ExecutionContext = context.dispatcher

    implicit val timeout = Timeout(5, TimeUnit.SECONDS)

    val rejectionHandler = ConsoleCommon.rejectionHandler

    val jsonPath = """(.+)\.json$""".r
    val formPath = """(.+)\.form$""".r

    val pluginContext = ConsoleEventServerPluginContext(logger)

    private lazy val base64Decoder = new BASE64Decoder

    case class AuthData(appId: Int, channelId: Option[Int], events: Seq[String])

    case class EngineAuthData(engineId: String, accessKey: String)

    def withSecurityKeyOnly: RequestContext => Future[Authentication[Unit]] = {
      ctx: RequestContext =>
        val securityKeyParamOpt = ctx.request.uri.query.get("securityKey")
        Future {
          Storage.getMetaDataServerConfigs.get().map{ svConfig =>
            securityKeyParamOpt.map{ securityKey =>
              if(securityKey == svConfig.securityKey){
                Right()
              } else {
                FailedAuth
              }
            }.getOrElse{
              ctx.request.headers.find(_.name == "Authorization").map { authHeader =>
                authHeader.value.split("Basic ") match {
                  case Array(_, value) =>
                    val securityKey =
                      new String(base64Decoder.decodeBuffer(value)).trim.split(":")(0)
                      if(securityKey == svConfig.securityKey){
                        Right()
                      } else {
                        FailedAuth
                      }
                  case _ => FailedAuth
                }
              }.getOrElse(FailedAuth)
            }
          }.getOrElse{
            logger.warning(s"No security key is configured")
            Right()
          }
        }
    }

    /* with accessKey in query/header, return appId if succeed */
    def withAccessKey: RequestContext => Future[Authentication[AuthData]] = {
      ctx: RequestContext =>
        val securityKeyParamOpt = ctx.request.uri.query.get("securityKey")
        val accessKeyParamOpt = ctx.request.uri.query.get("accessKey")
        val channelParamOpt = ctx.request.uri.query.get("channel")
        Future {
          // with accessKey in query, return appId if succeed
          securityKeyParamOpt.map { securityKey =>
            accessKeyParamOpt.map { accessKeyParam =>
              Storage.getMetaDataServerConfigs.get().map{ svConfig =>
                if(securityKey == svConfig.securityKey){
                  accessKeysClient.get(accessKeyParam).map { k =>
                    channelParamOpt.map { ch =>
                      val channelMap =
                        channelsClient.getByAppid(k.appid)
                        .map(c => (c.name, c.id)).toMap
                      if (channelMap.contains(ch)) {
                        Right(AuthData(k.appid, Some(channelMap(ch)), k.events))
                      } else {
                        Left(ChannelRejection(s"Invalid channel '$ch'."))
                      }
                    }.getOrElse{
                      Right(AuthData(k.appid, None, k.events))
                    }
                  }.getOrElse{
                        if(accessKeyParam == internalAccessKey){
                            Right(AuthData(0, None, Seq[String]()))
                        }else{
                          FailedAuth
                        }
                  }
                } else {
                  FailedAuth
                }
              }.getOrElse{
                FailedAuth
              }
            }.getOrElse{
              // with accessKey in header, return appId if succeed
              ctx.request.headers.find(_.name == "Authorization").map { authHeader =>
                authHeader.value.split("Basic ") match {
                  case Array(_, value) =>
                    val appAccessKey =
                      new String(base64Decoder.decodeBuffer(value)).trim.split(":")(0)
                      accessKeysClient.get(appAccessKey) match {
                        case Some(k) => Right(AuthData(k.appid, None, k.events))
                        case None => FailedAuth
                      }
                  case _ => FailedAuth
                }
              }.getOrElse(MissedAuth)
            }
          }.getOrElse {
            if(Storage.getMetaDataServerConfigs.get() == None){
              logger.warning(s"No Security Key is configured")
              accessKeyParamOpt.map { accessKeyParam =>
                accessKeysClient.get(accessKeyParam).map { k =>
                  channelParamOpt.map { ch =>
                    val channelMap =
                      channelsClient.getByAppid(k.appid)
                      .map(c => (c.name, c.id)).toMap
                    if (channelMap.contains(ch)) {
                      Right(AuthData(k.appid, Some(channelMap(ch)), k.events))
                    } else {
                      Left(ChannelRejection(s"Invalid channel '$ch'."))
                    }
                  }.getOrElse{
                    Right(AuthData(k.appid, None, k.events))
                  }
                }.getOrElse{
                        if(accessKeyParamOpt == internalAccessKey){
                            Right(AuthData(0, None, Seq[String]()))
                        }else{
                          FailedAuth
                        }
                  }
              }.getOrElse{
                // with accessKey in header, return appId if succeed
                ctx.request.headers.find(_.name == "Authorization").map { authHeader =>
                  authHeader.value.split("Basic ") match {
                    case Array(_, value) =>
                      val appAccessKey =
                        new String(base64Decoder.decodeBuffer(value)).trim.split(":")(0)
                        accessKeysClient.get(appAccessKey) match {
                          case Some(k) => Right(AuthData(k.appid, None, k.events))
                          case None => FailedAuth
                        }
                    case _ => FailedAuth
                  }
                }.getOrElse(MissedAuth)
              }
            }else{
              FailedAuth
            }
          }
        }
    }

    def startUpBaseEngines(): Unit = {
      //val allEngines = Storage.getMetaDataEngineInstances.getAll
      val allManifests = Storage.getMetaDataEngineManifests
      //val baseEngines = allEngines.filter(_.engineVariant == "base").map(x => x.engineId).distinct
      val baseEngines = Storage.getMetaDataEngineInstances.getBaseEngines()
      for(engine <- baseEngines){
          allManifests.get(engine, engine) map {manifest =>
            val port = manifest.port
            Future{
              System.out.println(s"Starting up base engine ${engine} at port ${port}")
              /**val stream = Process(Seq("pio", 
                                      "deploy", 
                                      s"--port $port", 
                                      s"--variant ${pio_root}/engines/${engine}/engine.json", 
                                      s"--engine-id ${engine}")).lines
              **/console.deploy(ConsoleArgs(deploy = DeployArgs(port = port), common = CommonArgs(engineId = s"$engine", variantJson = new File(s"${pio_root}/engines/${engine}/engine.json"))))
              // stream foreach println
            }
          } getOrElse {
            error(s"base engine ${engine} not found")
          }
      } 
    }

    def registerEngine(baseEngine: String):EngineAuthData = {
      val id = java.util.UUID.randomUUID().toString
      val accessKey = generateAccessKey()
      Future{
       // Process(Seq("pio", "register", s"--engine-id ${id}", s"--base-engine-url ${pio_root}/engines/${baseEngine}", s"--base-engine-id $baseEngine"),
       //   new File(s"${pio_root}/engines/${baseEngine}")).!
        var ca = ConsoleArgs(common = CommonArgs(engineId = id, baseEngineURL = s"${pio_root}/engines/${baseEngine}", baseEngineId = baseEngine))
        console.registerEngineWithArgs(ca, id)
        //Process(Seq("pio", "app", "new", id, "--access-key", accessKey)).!
        ca = ConsoleArgs(app = AppArgs(name = id), accessKey = AccessKeyArgs(accessKey = accessKey))
        App.create(ca);
      }
      EngineAuthData(engineId = id, accessKey = accessKey)
    }

    def generateAccessKey():String = {
      val sr = SecureRandom.getInstance("SHA1PRNG")
      val srBytes = Array.fill(48)(0.toByte)
      sr.nextBytes(srBytes)
      Base64.encodeBase64URLSafeString(srBytes)
    }

    def deleteEngine(engineId: String):String = {
      //Process(Seq("pio", "app", "delete", engineId, "-f")).!
      val ca = ConsoleArgs(common = CommonArgs(engineId = engineId), app = AppArgs(force = true))
      App.delete(ca)
      s"engine ${engineId} removed"
    }

    def deleteEngineData(engineId: String):String = {
      //Process(Seq("pio", "app", "data-delete", engineId, "-f")).!
      val ca = ConsoleArgs(common = CommonArgs(engineId = engineId), app = AppArgs(force = true))
      App.dataDelete(ca)
      s"event data for ${engineId} cleared"
    }

    def trainEngine(engineData: EngineData) = {
      if(trainingLeft > 0 && (trainingQueue.size == 0 || engineData.engineId == "")){
        var engineId = engineData.engineId
        if(engineId == ""){
          engineId = trainingQueue.dequeue().engineId
          System.out.println(s"Grabbed $engineId from queue")
        }
        val manifests = Storage.getMetaDataEngineManifests
        manifests.get(engineId, engineId) map {manifest =>
          val baseEngine = manifest.baseEngine 
          val newManifest = manifest.copy(trainingStatus = "INIT")
          manifests.update(newManifest)
          trainingLeft -= 1
          val training: Future[String] = Future {
            System.out.println(s"Training started for ${engineId}, number of training left is $trainingLeft")
            
            // val stream = Process(Seq(
            //   "pio", 
            //   "train", 
            //   s"--engine-id ${engineId}", 
            //   s"--base-engine-url ${pio_root}/engines/${baseEngine}", 
            //   s"--base-engine-id ${baseEngine}",
            //   s"--variant ${pio_root}/engines/engine-params/${engineId}.json")).lines 
            // /**new File(s"${pio_root}/engines/${baseEngine}")).lines**/
            // var output = ""
            // stream foreach println
            // for (line <- stream){
            //   output = line
            // }
            val ca = ConsoleArgs(common = CommonArgs(variantJson = new File(s"${pio_root}/engines/${baseEngine}/engine.json"), 
                                                    engineId = engineId, 
                                                    baseEngineURL = s"${pio_root}/engines/${baseEngine}", 
                                                    baseEngineId = baseEngine,
                                                    pioHome = Some(s"${pio_root}/PredictionIO-0.10.0-incubating")))
            console.train(ca)
            engineId
          }

          training onComplete {
            case Success(id) => {
                                
                                trainingLeft += 1
                                System.out.println(s"training finished for ${id}, number of training left is $trainingLeft")
                                checkForTrainingJobs()
                                }
            case Failure(t) => {
                                trainingLeft += 1
                                val newManifest = manifest.copy(trainingStatus = "FAILED")
                                manifests.update(newManifest)
                                error("An error has occured at train: " + t.getMessage)
                                System.out.println(s"training failed, number of training left is $trainingLeft")
                                checkForTrainingJobs()
                                }
          }
        } getOrElse {
          error(s"No engineManifest can be found for $engineId")
        }
      }else{
        if(engineData.engineId != ""){
          val manifests = Storage.getMetaDataEngineManifests
            manifests.get(engineData.engineId, engineData.engineId) map {manifest =>
              val baseEngine = manifest.baseEngine 
              val newManifest = manifest.copy(trainingStatus = "INIT")
              manifests.update(newManifest)
              System.out.println(s"Training queued for ${engineData.engineId}")
              trainingQueue.enqueue(engineData)
          }
          
        }      
      }
      
    }

    def checkForTrainingJobs() = {
      implicit val formats = DefaultFormats.lossless

      val data = Map(
                    "engineId" -> "",
                    "accessKey" -> "",
                    "baseEngine" -> "")
      val serverConfig = Storage.getMetaDataServerConfigs.get().getOrElse(new ServerConfig())
      val securityKey = serverConfig.securityKey

      val f: Future[Int] = Future {
        scalaj.http.Http(
          s"http://localhost:7070/" +
          s"engine/train?accessKey=${internalAccessKey}&&securityKey=$securityKey").postData(
            write(data)
          ).header(
            "content-type", "application/json"
          ).asString.code
      }
    }

    def getTrainStatus(engineId: String):String = {
      val manifests = Storage.getMetaDataEngineManifests
      var status = ""
      manifests.get(engineId, engineId) map {em => 
        status = em.trainingStatus      
      } getOrElse {
        error(s"No engine manifest found for $engineId")
      }
      status
    }

    private val FailedAuth = Left(
      AuthenticationFailedRejection(
        AuthenticationFailedRejection.CredentialsRejected, List()
      )
    )

    private val MissedAuth = Left(
      AuthenticationFailedRejection(
        AuthenticationFailedRejection.CredentialsMissing, List()
      )
    )

    //lazy val statsActorRef = actorRefFactory.actorSelection("/user/StatsActor")
    lazy val pluginsActorRef = actorRefFactory.actorSelection("/user/PluginsActor")

    val route: Route =
      pathSingleSlash {
        import Json4sProtocol._

        get {
          respondWithMediaType(MediaTypes.`application/json`) {
            complete(Map("status" -> "alive"))
          }
        }
      } ~
      path("events.json") {

        import Json4sProtocol._

        post {
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withAccessKey) { authData =>
                val appId = authData.appId
                val channelId = authData.channelId
                val events = authData.events
                entity(as[Event]) { event =>
                  complete {
                    if (events.isEmpty || authData.events.contains(event.event)) {
                      pluginContext.inputBlockers.values.foreach(
                        _.process(ConsoleEventInfo(
                          appId = appId,
                          channelId = channelId,
                          event = event), pluginContext))
                      val data = eventClient.futureInsert(event, appId, channelId).map { id =>
                        pluginsActorRef ! ConsoleEventInfo(
                          appId = appId,
                          channelId = channelId,
                          event = event)
                        val result = (StatusCodes.Created, Map("eventId" -> s"${id}"))
                        //if (config.stats) {
                        //  statsActorRef ! Bookkeeping(appId, result._1, event)
                        //}
                        result
                      }
                      data
                    } else {
                      (StatusCodes.Forbidden,
                        Map("message" -> s"${event.event} events are not allowed"))
                    }
                  }
                }
              }
            }
          }
        } ~
        get {
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withAccessKey) { authData =>
                val appId = authData.appId
                val channelId = authData.channelId
                parameters(
                  'startTime.as[Option[String]],
                  'untilTime.as[Option[String]],
                  'entityType.as[Option[String]],
                  'entityId.as[Option[String]],
                  'event.as[Option[String]],
                  'targetEntityType.as[Option[String]],
                  'targetEntityId.as[Option[String]],
                  'limit.as[Option[Int]],
                  'reversed.as[Option[Boolean]]) {
                  (startTimeStr, untilTimeStr, entityType, entityId,
                    eventName,  // only support one event name
                    targetEntityType, targetEntityId,
                    limit, reversed) =>
                  respondWithMediaType(MediaTypes.`application/json`) {
                    complete {
                      logger.debug(
                        s"GET events of appId=${appId} " +
                        s"st=${startTimeStr} ut=${untilTimeStr} " +
                        s"et=${entityType} eid=${entityId} " +
                        s"li=${limit} rev=${reversed} ")

                      require(!((reversed == Some(true))
                        && (entityType.isEmpty || entityId.isEmpty)),
                        "the parameter reversed can only be used with" +
                        " both entityType and entityId specified.")

                      val parseTime = Future {
                        val startTime = startTimeStr.map(SUtils.stringToDateTime(_))
                        val untilTime = untilTimeStr.map(SUtils.stringToDateTime(_))
                        (startTime, untilTime)
                      }


                      parseTime.flatMap { case (startTime, untilTime) =>
                        val data = eventClient.futureFind(
                          appId = appId,
                          channelId = channelId,
                          startTime = startTime,
                          untilTime = untilTime,
                          entityType = entityType,
                          entityId = entityId,
                          eventNames = eventName.map(List(_)),
                          targetEntityType = targetEntityType.map(Some(_)),
                          targetEntityId = targetEntityId.map(Some(_)),
                          limit = limit.orElse(Some(20)),
                          reversed = reversed)
                          .map { eventIter =>
                            if (eventIter.hasNext) {
                              (StatusCodes.OK, eventIter.toArray)
                            } else {
                              (StatusCodes.NotFound,
                                Map("message" -> "Not Found"))
                            }
                          }
                        data
                      }.recover {
                        case e: Exception =>
                          (StatusCodes.BadRequest, Map("message" -> s"${e}"))
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } ~
      path("engine" / "register") {
        import Json4sProtocol._
        post{
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withSecurityKeyOnly) { _ =>
                entity(as[EngineData]) {data =>
                  val baseEngine = data.baseEngine
                  val engineAuthData = registerEngine(baseEngine)
                  val formattedData = Map("engineId" -> engineAuthData.engineId,
                                          "accessKey" -> engineAuthData.accessKey)
                  respondWithMediaType(MediaTypes.`application/json`) {
                    complete(formattedData)
                  }
                }
              }
            }
          }
        }~
        delete {
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) { 
              authenticate(withAccessKey) { authData =>
                entity(as[EngineData]) {data =>
                  complete {
                    val engineId = data.engineId
                    deleteEngine(engineId)
                  }
                }
              }
            }
          }
        }  
      }~
      path("engine" / "data"){
        import Json4sProtocol._
        get {
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withAccessKey) {authData =>
                val entityIdLst = eventClient.getEntityIds(authData.appId, authData.channelId)
                val payload = Map("entityIds" -> entityIdLst)
                respondWithMediaType(MediaTypes.`application/json`) {
                  complete(payload)
                }                           
              }
            }
          }
        }~
        delete {
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withAccessKey) {authData =>
                entity(as[EngineData]) {data =>
                  complete {
                    val engineId = data.engineId
                    deleteEngineData(engineId)
                  }
                }
              }
            }
          }
        }
      }~
      path("engine" / "train"){
        import Json4sProtocol._
        post{
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withAccessKey) { _ =>
                entity(as[EngineData]) {data =>
                  complete {
                    trainEngine(data)
                    s"training started for engine ${data.engineId}"
                  }
                }
              }
            }
          }
        }
      }~
      path("engine" / "status"){
        import Json4sProtocol._
        post{
          handleExceptions(ConsoleCommon.exceptionHandler) {
            handleRejections(rejectionHandler) {
              authenticate(withAccessKey){ _ =>
                entity(as[EngineData]) {data =>
                  val engineId = data.engineId
                  val status = getTrainStatus(engineId)
                  val formatedData = Map("status" -> status)
                  respondWithMediaType(MediaTypes.`application/json`) {
                    complete(formatedData)
                  }
                }
              }
            }
          }
        }
      }

    def receive: Actor.Receive = {
      startUpBaseEngines()
      runRoute(route)
    }
  }



  /* message */
  case class StartServer(host: String, port: Int)

  class ConsoleEventServerActor(
      val eventClient: LEvents,
      val accessKeysClient: AccessKeys,
      val channelsClient: Channels,
      val config: ConsoleEventServerConfig) extends Actor with ActorLogging {
    val child = context.actorOf(
      Props(classOf[ConsoleEventServiceActor],
        eventClient,
        accessKeysClient,
        channelsClient,
        config),
      "ConsoleEventServiceActor")
    implicit val system = context.system

    def receive: Actor.Receive = {
      case StartServer(host, portNum) => {
        IO(SHttp) ! SHttp.Bind(child, interface = host, port = portNum)
      }
      case m: SHttp.Bound => log.info("Bound received. ConsoleEventServer is ready.")
      case m: SHttp.CommandFailed => log.error("Command failed.")
      case _ => log.error("Unknown message.")
    }
  }

  case class ConsoleEventServerConfig(
    ip: String = "localhost",
    port: Int = 7070,
    plugins: String = "plugins",
    stats: Boolean = false)

  object ConsoleEventServer {
    def createEventServer(config: ConsoleEventServerConfig): Unit = {
      implicit val system = ActorSystem("ConsoleEventServerSystem")

      val eventClient = Storage.getLEvents()
      val accessKeysClient = Storage.getMetaDataAccessKeys()
      val channelsClient = Storage.getMetaDataChannels()

      val serverActor = system.actorOf(
        Props(
          classOf[ConsoleEventServerActor],
          eventClient,
          accessKeysClient,
          channelsClient,
          config),
        "ConsoleEventServerActor"
      )
      //if (config.stats) system.actorOf(Props[ConsoleStatsActor], "ConsoleStatsActor")
      system.actorOf(Props[ConsolePluginsActor], "ConsolePluginsActor")
      serverActor ! StartServer(config.ip, config.port)
      system.awaitTermination()
    }
  }

  object Run {
    def main(args: Array[String]) {
      ConsoleEventServer.createEventServer(ConsoleEventServerConfig(
        ip = "0.0.0.0",
        port = 7070))
    }
  }
}
