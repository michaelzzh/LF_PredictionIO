package org.apache.predictionio.workflow

import java.io.PrintWriter
import java.io.Serializable
import java.io.StringWriter
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.event.Logging
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.github.nscala_time.time.Imports.DateTime
import com.twitter.bijection.Injection
import com.twitter.chill.KryoBase
import com.twitter.chill.KryoInjection
import com.twitter.chill.ScalaKryoInstantiator
import com.typesafe.config.ConfigFactory
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer
import grizzled.slf4j.Logging
import org.apache.predictionio.authentication.KeyAuthentication
import org.apache.predictionio.configuration.SSLConfiguration
import org.apache.predictionio.controller.Engine
import org.apache.predictionio.controller.Params
import org.apache.predictionio.controller.Utils
import org.apache.predictionio.controller.WithPrId
import org.apache.predictionio.core.BaseAlgorithm
import org.apache.predictionio.core.BaseServing
import org.apache.predictionio.core.Doer
import org.apache.predictionio.data.storage.EngineInstance
import org.apache.predictionio.data.storage.EngineManifest
import org.apache.predictionio.data.storage.ClientManifest
import org.apache.predictionio.data.storage.QueryHistory
import org.apache.predictionio.data.storage.Storage
import org.apache.predictionio.data.storage.QueryData
import org.apache.predictionio.data.storage.QueryEntry
import org.apache.predictionio.data.storage.ResultEntry
import org.apache.predictionio.data.api.Common
import org.apache.predictionio.workflow.JsonExtractorOption.JsonExtractorOption
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
import spray.can.Http
import spray.can.server.ServerSettings
import spray.http.FormData
import spray.http.MediaTypes._
import spray.http._
import spray.httpx.Json4sSupport
import spray.routing._
import spray.routing.authentication.{UserPass, BasicAuth}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.future
import scala.language.existentials
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scalaj.http.HttpOptions
import scala.collection.mutable.ListBuffer

import scopt.Read
import scopt._

import scala.collection.mutable.ListBuffer

class ESKryoInstantiator(classLoader: ClassLoader) extends ScalaKryoInstantiator {
	def newkryo(): KryoBase = {
		val kryo = super.newKryo()
		kryo.setClassLoader(classLoader)
		SynchronizedCollectionsSerializer.registerSerializers(kryo)
		kryo
	}
}

object ESKryoInstantiator extends Serializable {
	def newKryoInjection : Injection[Any, Array[Byte]] = {
		val kryoInstantiator = new KryoInstantiator(getClass.getClassLoader)
		KryoInjection.instance(kryoInstantiator)
	}
}

case class EngineServerConfig(
  batch: String = "",
  engineInstanceId: String = "",
  engineId: Option[String] = None,
  engineVersion: Option[String] = None,
  engineVariant: String = "",
  env: Option[String] = None,
  ip: String = "0.0.0.0",
  port: Int = 8000,
  feedback: Boolean = false,
  eventServerIp: String = "0.0.0.0",
  eventServerPort: Int = 7070,
  accessKey: Option[String] = None,
  logUrl: Option[String] = None,
  logPrefix: Option[String] = None,
  logFile: Option[String] = None,
  verbose: Boolean = false,
  debug: Boolean = false,
  jsonExtractor: JsonExtractorOption = JsonExtractorOption.Both)

case class StartEngineServer()
case class BindEngineServer()
case class StopEngineServer()
case class ReloadEngineServer()

object EngineServer extends Logging {
  val actorSystem = ActorSystem("pio-server")
  val engineInstances = Storage.getMetaDataEngineInstances
  val engineManifests = Storage.getMetaDataEngineManifests
  val modeldata = Storage.getModelDataModels

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[EngineServerConfig]("EngineServer") {
      opt[String]("batch") action { (x, c) =>
        c.copy(batch = x)
      } text("Batch label of the deployment.")
      opt[String]("engineId") action { (x, c) =>
        c.copy(engineId = Some(x))
      } text("Engine ID.")
      opt[String]("engineVersion") action { (x, c) =>
        c.copy(engineVersion = Some(x))
      } text("Engine version.")
      opt[String]("engine-variant") required() action { (x, c) =>
        c.copy(engineVariant = x)
      } text("Engine variant JSON.")
      opt[String]("ip") action { (x, c) =>
        c.copy(ip = x)
      }
      opt[String]("env") action { (x, c) =>
        c.copy(env = Some(x))
      } text("Comma-separated list of environmental variables (in 'FOO=BAR' " +
        "format) to pass to the Spark execution environment.")
      opt[Int]("port") action { (x, c) =>
        c.copy(port = x)
      } text("Port to bind to (default: 8000).")
      opt[String]("engineInstanceId") required() action { (x, c) =>
        c.copy(engineInstanceId = x)
      } text("Engine instance ID.")
      opt[Unit]("feedback") action { (_, c) =>
        c.copy(feedback = true)
      } text("Enable feedback loop to event server.")
      opt[String]("event-server-ip") action { (x, c) =>
        c.copy(eventServerIp = x)
      }
      opt[Int]("event-server-port") action { (x, c) =>
        c.copy(eventServerPort = x)
      } text("Event server port. Default: 7070")
      opt[String]("accesskey") action { (x, c) =>
        c.copy(accessKey = Some(x))
      } text("Event server access key.")
      opt[String]("log-url") action { (x, c) =>
        c.copy(logUrl = Some(x))
      }
      opt[String]("log-prefix") action { (x, c) =>
        c.copy(logPrefix = Some(x))
      }
      opt[String]("log-file") action { (x, c) =>
        c.copy(logFile = Some(x))
      }
      opt[Unit]("verbose") action { (x, c) =>
        c.copy(verbose = true)
      } text("Enable verbose output.")
      opt[Unit]("debug") action { (x, c) =>
        c.copy(debug = true)
      } text("Enable debug output.")
      opt[String]("json-extractor") action { (x, c) =>
        c.copy(jsonExtractor = JsonExtractorOption.withName(x))
      }
    }

    parser.parse(args, EngineServerConfig()) map { sc =>
      WorkflowUtils.modifyLogging(sc.verbose)
      engineInstances.get(sc.engineInstanceId) map { engineInstance =>
        val engineId = sc.engineId.getOrElse(engineInstance.engineId)
        val engineVersion = sc.engineVersion.getOrElse(
          engineInstance.engineVersion)
        engineManifests.get(engineId, engineVersion) map { manifest =>
          if(manifest.port == -1){
            error("non-base engines are not deployable")
          }
          val engineFactoryName = engineInstance.engineFactory
          val master = actorSystem.actorOf(Props(
            classOf[EngineServerMasterActor],
            sc,
            engineInstance,
            engineFactoryName,
            manifest),
          "master")
          implicit val timeout = Timeout(5.seconds)
          master ? StartEngineServer()
          actorSystem.awaitTermination
        } getOrElse {
          error(s"Invalid engine ID or version. Aborting server.")
        }
      } getOrElse {
        error(s"Invalid engine instance ID. Aborting server.")
      }
    }
  }

  def createEngineServerActorWithEngine[TD, EIN, PD, Q, P, A](
    sc: EngineServerConfig,
    engineInstance: EngineInstance,
    engine: Engine[TD, EIN, PD, Q, P, A],
    engineLanguage: EngineLanguage.Value): ActorRef = {

    val engineParams = engine.engineInstanceToEngineParams(engineInstance, sc.jsonExtractor)

    val kryo = KryoInstantiator.newKryoInjection

    val batch = if (engineInstance.batch.nonEmpty) {
      s"${engineInstance.engineFactory} (${engineInstance.batch})"
    } else {
      engineInstance.engineFactory
    }

    val sparkContext = WorkflowContext(
      batch = batch,
      executorEnv = engineInstance.env,
      mode = "Serving",
      sparkEnv = engineInstance.sparkConf)

    actorSystem.actorOf(
      Props(
        classOf[EngineServerActor[Q, P]],
        sc,
        engine,
        sparkContext))
  }
}

class EngineServerMasterActor (
    sc: EngineServerConfig,
    engineInstance: EngineInstance,
    engineFactoryName: String,
    manifest: EngineManifest) extends Actor with SSLConfiguration with KeyAuthentication {
  val log = Logging(context.system, this)
  implicit val system = context.system
  var sprayHttpListener: Option[ActorRef] = None
  var currentServerActor: Option[ActorRef] = None
  var retry = 3
  val serverConfig = ConfigFactory.load("server.conf")
  val sslEnforced = serverConfig.getBoolean("org.apache.predictionio.server.ssl-enforced")
  val protocol = if (sslEnforced) "https://" else "http://"

  def undeploy(ip: String, port: Int): Unit = {
    val serverUrl = s"${protocol}${ip}:${port}"
    log.info(
      s"Undeploying any existing engine instance at $serverUrl")
    try {
      val code = scalaj.http.Http(s"$serverUrl/stop")
        .option(HttpOptions.allowUnsafeSSL)
        .param(ServerKey.param, ServerKey.get)
        .method("POST").asString.code
      code match {
        case 200 => ()
        case 404 => log.error(
          s"Another process is using $serverUrl. Unable to undeploy.")
        case _ => log.error(
          s"Another process is using $serverUrl, or an existing " +
          s"engine server is not responding properly (HTTP $code). " +
          "Unable to undeploy.")
      }
    } catch {
      case e: java.net.ConnectException =>
        log.warning(s"Nothing at $serverUrl")
      case _: Throwable =>
        log.error("Another process might be occupying " +
          s"$ip:$port. Unable to undeploy.")
    }
  }

  def receive: Actor.Receive = {
    case x: StartEngineServer =>
      val actor = createEngineServerActor(
        sc,
        engineInstance,
        engineFactoryName,
        manifest)
      currentServerActor = Some(actor)
      undeploy(sc.ip, sc.port)
      self ! BindEngineServer()
    case x: BindEngineServer =>
      currentServerActor map { actor =>
        val settings = ServerSettings(system)
        IO(Http) ! Http.Bind(
          actor,
          interface = sc.ip,
          port = sc.port,
          settings = Some(settings.copy(sslEncryption = sslEnforced)))
      } getOrElse {
        log.error("Cannot bind a non-existing server backend.")
      }
    case x: StopEngineServer =>
      System.out.println("In stopEngineServer")
      log.info(s"Stop server command received.")
      sprayHttpListener.map { l =>
        log.info("Server is shutting down.")
        l ! Http.Unbind(5.seconds)
        system.shutdown
      } getOrElse {
        log.warning("No active server is running.")
      }
    case x: ReloadEngineServer =>
      log.info("Reload server command received.")
      val latestEngineInstance =
        EngineServer.engineInstances.getLatestCompleted(
          manifest.id,
          manifest.version,
          engineInstance.engineVariant)
      latestEngineInstance map { lr =>
        val actor = createEngineServerActor(sc, lr, engineFactoryName, manifest)
        sprayHttpListener.map { l =>
          l ! Http.Unbind(5.seconds)
          val settings = ServerSettings(system)
          IO(Http) ! Http.Bind(
            actor,
            interface = sc.ip,
            port = sc.port,
            settings = Some(settings.copy(sslEncryption = sslEnforced)))
          currentServerActor.get ! Kill
          currentServerActor = Some(actor)
        } getOrElse {
          log.warning("No active server is running. Abort reloading.")
        }
      } getOrElse {
        log.warning(
          s"No latest completed engine instance for ${manifest.id} " +
          s"${manifest.version}. Abort reloading.")
      }
    case x: Http.Bound =>
      val serverUrl = s"${protocol}${sc.ip}:${sc.port}"
      log.info(s"Engine is deployed and running. Engine API is live at ${serverUrl}.")
      sprayHttpListener = Some(sender)
    case x: Http.CommandFailed =>
      if (retry > 0) {
        retry -= 1
        log.error(s"Bind failed. Retrying... ($retry more trial(s))")
        context.system.scheduler.scheduleOnce(1.seconds) {
          self ! BindEngineServer()
        }
      } else {
        log.error("Bind failed. Shutting down.")
        system.shutdown
      }
  }

  def createEngineServerActor(
      sc: EngineServerConfig,
      engineInstance: EngineInstance,
      engineFactoryName: String,
      manifest: EngineManifest): ActorRef = {
    val (engineLanguage, engineFactory) =
      WorkflowUtils.getEngine(engineFactoryName, getClass.getClassLoader)
    val engine = engineFactory()

    // EngineFactory return a base engine, which may not be deployable.
    if (!engine.isInstanceOf[Engine[_,_,_,_,_,_]]) {
      throw new NoSuchMethodException(s"Engine $engine is not deployable")
    }

    val deployableEngine = engine.asInstanceOf[Engine[_,_,_,_,_,_]]

    EngineServer.createEngineServerActorWithEngine(
      sc,
      engineInstance,
      // engine,
      deployableEngine,
      engineLanguage)
  }
}

class EngineServerActor[Q, P](
    val args: EngineServerConfig,
    val engine: Engine[_, _, _, Q, P, _],
    val sparkContext: SparkContext) extends Actor with HttpService with KeyAuthentication {
  
  val kryo = ESKryoInstantiator.newKryoInjection
  val serverStartTime = DateTime.now
  val log = Logging(context.system, this)

  var requestCount: Int = 0
  var avgServingSec: Double = 0.0
  var lastServingSec: Double = 0.0

  /** The following is required by HttpService */
  def actorRefFactory: ActorContext = context

  implicit val timeout = Timeout(5, TimeUnit.SECONDS)
  val pluginsActorRef =
    context.actorOf(Props(classOf[PluginsActor], args.engineVariant), "PluginsActor")
  val pluginContext = EngineServerPluginContext(log, args.engineVariant)

  def receive: Actor.Receive = runRoute(myRoute)

  val feedbackEnabled = if (args.feedback) {
    if (args.accessKey.isEmpty) {
      log.error("Feedback loop cannot be enabled because accessKey is empty.")
      false
    } else {
      true
    }
  } else false

  def remoteLog(logUrl: String, logPrefix: String, message: String): Unit = {
    implicit val formats = Utils.json4sDefaultFormats
    try {
      scalaj.http.Http(logUrl).postData(
        logPrefix + write(Map(
          "message" -> message))).asString
    } catch {
      case e: Throwable =>
        log.error(s"Unable to send remote log: ${e.getMessage}")
    }
  }

  def getStackTraceString(e: Throwable): String = {
    val writer = new StringWriter()
    val printWriter = new PrintWriter(writer)
    e.printStackTrace(printWriter)
    writer.toString
  }

  val myRoute =
    path("queries.json") {
      import ServerJson4sSupport._
      post {
        handleExceptions(Common.exceptionHandler) {
         handleRejections(Common.rejectionHandler) {
          entity(as[QueryData]) { qd =>
            val jsonExtractorOption = args.jsonExtractor
            val requestStartTime = DateTime.now
          	val engineId = qd.engineId
            val queries = qd.properties
            val clientId = qd.clientId
            try {
              var engineInstanceId = ""

              val client = Storage.getMetaDataClientManifests.get(clientId) getOrElse {error(s"No existing client for client id $clientId")}
              val replyAddress = client.url
              val modeldata = Storage.getModelDataModels
              val queryHistories = Storage.getHistoryDataQueryHistories
              val engineInstances = Storage.getMetaDataEngineInstances
      		    val engineInstance = engineInstances.getLatestCompleted(engineId, engineId, "default") getOrElse {error(
            			s"No valid engine instance found for engine ${engineId} "
            		)}
      		    engineInstanceId = engineInstance.id

      		    val engineParams = engine.engineInstanceToEngineParams(engineInstance, args.jsonExtractor)

      		    val algorithms = engineParams.algorithmParamsList.map { case (n, p) =>
      				  Doer(engine.algorithmClassMap(n), p)
    		      }

    		      val servingParamsWithName = engineParams.servingParams

    		      val serving = Doer(engine.servingClassMap(servingParamsWithName._1),
      		  	servingParamsWithName._2)

      		    val modelsFromEngineInstance = kryo.invert(modeldata.get(engineInstance.id).get.models).get.asInstanceOf[Seq[Any]]
      		    val models = engine.prepareDeploy(
      		  	  sparkContext,
      		  	  engineParams,
      		  	  engineInstanceId,
      		  	  modelsFromEngineInstance,
      		  	  params = WorkflowParams()
      		    )


              Future{
                var responseList = ListBuffer[ResultEntry]()
                for(singleQuery <- queries){
                  val queryString = singleQuery.queryString
                  val queryId = singleQuery.queryId

                  queryHistories.get(queryId) map {queryHistory =>
                    //if queryId already exists in the db, grab the result directly
                    val resultEntry = ResultEntry(queryId = queryId,
                                            resultString = queryHistory.result)
                    responseList += resultEntry
                  } getOrElse {
                    val singleServingStartTime = DateTime.now

                    val queryHistory = QueryHistory(id = queryId,
                                                status = "INIT",
                                                query = queryString,
                                                result = "")
                    queryHistories.insert(queryHistory)

                    // Extract Query from Json
                    val query = JsonExtractor.extract(
                      jsonExtractorOption,
                      queryString,
                      algorithms.head.queryClass,
                      algorithms.head.querySerializer,
                      algorithms.head.gsonTypeAdapterFactories
                    )
                    val queryJValue = JsonExtractor.toJValue(
                      jsonExtractorOption,
                      query,
                      algorithms.head.querySerializer,
                      algorithms.head.gsonTypeAdapterFactories)

                    // Deploy logic. First call Serving.supplement, then Algo.predict,
                    // finally Serving.serve.
                    val supplementedQuery = serving.supplementBase(query)
                    // TODO: Parallelize the following.
                    val predictions = algorithms.zipWithIndex.map { case (a, ai) =>
                      a.predictBase(models(ai), supplementedQuery)
                    }
                    // Notice that it is by design to call Serving.serve with the
                    // *original* query.
                    val prediction = serving.serveBase(query, predictions)
                    val predictionJValue = JsonExtractor.toJValue(
                      jsonExtractorOption,
                      prediction,
                      algorithms.head.querySerializer,
                      algorithms.head.gsonTypeAdapterFactories)

                    val result = predictionJValue

                    val pluginResult =
                      pluginContext.outputBlockers.values.foldLeft(result) { case (r, p) =>
                        p.process(engineInstance, queryJValue, r, pluginContext)
                      }

                    val newHist = queryHistory.copy(status = "COMPLETE",
                                    result = compact(render(pluginResult)))

                    queryHistories.update(newHist)

                    val resultEntry = ResultEntry(queryId = queryId,
                                                resultString = compact(render(pluginResult)))

                    responseList += resultEntry

                    // Bookkeeping
                    val servingEndTime = DateTime.now
                    lastServingSec =
                      (servingEndTime.getMillis - singleServingStartTime.getMillis) * 1.0
                    avgServingSec =
                      ((avgServingSec * requestCount) + lastServingSec) /
                      (requestCount + 1)
                    requestCount += 1
                      //System.out.println(s"query time: ${lastServingSec}, average query time: ${avgServingSec}")
                  }
                }

                val data = Map(
                  // "appId" -> dataSourceParams.asInstanceOf[ParamsWithAppId].appId,
                  "response" -> responseList.toList.map(result => Map("queryId" -> result.queryId,

                                                                      "resultString" -> result.resultString))
                )
                // At this point args.accessKey should be Some(String).
                val f: Future[Int] = future {
                  scalaj.http.Http(
                    s"${replyAddress}").postData(
                    write(data)).header(
                    "content-type", "application/json").asString.code
                }
                f onComplete {
                  case Success(code) => {
                    if (code != 201) {
                      log.error(s"send back response failed. Status code: $code."
                        + s"Data: ${write(data)}.")
                    }
                  }
                  case Failure(t) => {
                    log.error(s"send back response failed: ${t.getMessage}") }
                  }
                val requestEndTime = DateTime.now
                val totalRequestTime =
                  (requestEndTime.getMillis - requestStartTime.getMillis) / 1000.0
                System.out.println(s"total serving time: ${totalRequestTime} seconds, average query time: ${avgServingSec} milliseconds, items in this batch: ${queries.size}")
              }
              complete("query being performed")
              // respondWithMediaType(`application/json`) {
              //   complete(compact(render(pluginResult)))
              // }
            } catch {
              case e: MappingException =>
                log.error(
                  s"Query 'queryString' is invalid. Reason: ${e.getMessage}")
                args.logUrl map { url =>
                  remoteLog(
                    url,
                    args.logPrefix.getOrElse(""),
                    s"Query:\nqueryString\n\nStack Trace:\n" +
                      s"${getStackTraceString(e)}\n\n")
                  }
                complete(StatusCodes.BadRequest, e.getMessage)
              case e: Throwable =>
                val msg = s"Query:\nqueryString\n\nStack Trace:\n" +
                  s"${getStackTraceString(e)}\n\n"
                log.error(msg)
                args.logUrl map { url =>
                  remoteLog(
                    url,
                    args.logPrefix.getOrElse(""),
                    msg)
                  }
                complete(StatusCodes.InternalServerError, msg)
              }
            }
          }
        }
      }
    } ~
    path("reload") {
      authenticate(withAccessKeyFromFile) { request =>
        post {
          complete {
            context.actorSelection("/user/master") ! ReloadEngineServer()
            "Reloading..."
          }
        }
      }
    } ~
    path("stop") {
      authenticate(withAccessKeyFromFile) { request =>
        post {
          complete {
            context.system.scheduler.scheduleOnce(1.seconds) {
              context.actorSelection("/user/master") ! StopEngineServer()
            }
            "Shutting down..."
          }
        }
      }
    } ~
    pathPrefix("assets") {
      getFromResourceDirectory("assets")
    } ~
    path("plugins.json") {
      import ServerJson4sSupport._
      get {
        respondWithMediaType(MediaTypes.`application/json`) {
          complete {
            Map("plugins" -> Map(
              "outputblockers" -> pluginContext.outputBlockers.map { case (n, p) =>
                n -> Map(
                  "name" -> p.pluginName,
                  "description" -> p.pluginDescription,
                  "class" -> p.getClass.getName,
                  "params" -> pluginContext.pluginParams(p.pluginName))
              },
              "outputsniffers" -> pluginContext.outputSniffers.map { case (n, p) =>
                n -> Map(
                  "name" -> p.pluginName,
                  "description" -> p.pluginDescription,
                  "class" -> p.getClass.getName,
                  "params" -> pluginContext.pluginParams(p.pluginName))
              }
            ))
          }
        }
      }
    } ~
    path("plugins" / Segments) { segments =>
      import ServerJson4sSupport._
      get {
        respondWithMediaType(MediaTypes.`application/json`) {
          complete {
            val pluginArgs = segments.drop(2)
            val pluginType = segments(0)
            val pluginName = segments(1)
            pluginType match {
              case EngineServerPlugin.outputSniffer =>
                pluginsActorRef ? PluginsActor.HandleREST(
                  pluginName = pluginName,
                  pluginArgs = pluginArgs) map {
                  _.asInstanceOf[String]
                }
            }
          }
        }
      }
    }
}

object ServerJson4sSupport extends Json4sSupport {
  implicit def json4sFormats: Formats = DefaultFormats
}


