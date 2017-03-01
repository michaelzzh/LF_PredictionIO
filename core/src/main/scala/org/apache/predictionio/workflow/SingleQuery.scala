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
import org.apache.predictionio.data.storage.Storage
import org.apache.predictionio.workflow.JsonExtractorOption.JsonExtractorOption
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.future
import scala.language.existentials
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scalaj.http.HttpOptions

class SQKryoInstantiator(classLoader: ClassLoader) extends ScalaKryoInstantiator {
	def newkryo(): KryoBase = {
		val kryo = super.newKryo()
		kryo.setClassLoader(classLoader)
		SynchronizedCollectionsSerializer.registerSerializers(kryo)
		kryo
	}
}

object SQKryoInstantiator extends Serializable {
	def newKryoInjection : Injection[Any, Array[Byte]] = {
		val kryoInstantiator = new KryoInstantiator(getClass.getClassLoader)
		KryoInjection.instance(kryoInstantiator)
	}
}

case class QueryConfig(
  engineInstanceId: String = "",
  engineVariant: String = "",
  eventServerIp: String = "0.0.0.0",
  eventServerPort: Int = 7070,
  batch: String = "",
  engineId: Option[String] = None,
  engineVersion: Option[String] = None,
  env: Option[String] = None,
  ip: String = "0,0,0,0",
  port: Int = 8000,
  feedback: Boolean = false,
  accessKey: Option[String] = None,
  logUrl: Option[String] = None,
  logPrefix: Option[String] = None,
  logFile: Option[String] = None,
  verbose: Boolean = false,
  debug: Boolean = false,
  jsonExtractor: JsonExtractorOption = JsonExtractorOption.Both)

object SingleQuery extends Logging {
	val engineInstances = Storage.getMetaDataEngineInstances
	val engineManifests = Storage.getMetaDataEngineManifests
	val modeldata = Storage.getModelDataModels

	def main(args: Array[String]): Unit = {
    	val parser = new scopt.OptionParser[QueryConfig]("DoQuery") {
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

    	parser.parse(args, QueryConfig()) map { qc =>
      	engineInstances.get(qc.engineInstanceId) map { engineInstance =>
        	val engineId = qc.engineId.getOrElse(engineInstance.engineId)
        	val engineVersion = qc.engineVersion.getOrElse(
          	engineInstance.engineVersion)
        	engineManifests.get(engineId, engineVersion) map { manifest =>
          		val engineFactoryName = engineInstance.engineFactory
         		val result = fetchEngineDataThenQuery(qc, engineInstance, engineFactoryName, manifest)
         		System.out.println(result)
        		} getOrElse {
        	  		error(s"Invalid engine ID or version. Aborting server.")
        		}
      		} getOrElse {
        		error(s"Invalid engine instance ID. Aborting server.")
      		}
    	}
  	}

	def doQuery(
		engineInstanceId: String = "",
		engineVariant: String = "",
		eventServerIp: String = "0,0,0,0",
		eventServerPort:Int = 7070): Int = {

		System.out.println("doQuery received command")

		val qc = QueryConfig(engineInstanceId, engineVariant, eventServerIp, eventServerPort)

		WorkflowUtils.modifyLogging(qc.verbose)
      	engineInstances.get(qc.engineInstanceId) map { engineInstance =>
        val engineId = qc.engineId.getOrElse(engineInstance.engineId)
        val engineVersion = qc.engineVersion.getOrElse(
          engineInstance.engineVersion)

        engineManifests.get(engineId, engineVersion) map { manifest =>
        val engineFactoryName = engineInstance.engineFactory
        val result = fetchEngineDataThenQuery(qc, engineInstance, engineFactoryName, manifest)
        System.out.println()
        //   val master = actorSystem.actorOf(Props(
        //     classOf[MasterActor],
        //     sc,
        //     engineInstance,
        //     engineFactoryName,
        //     manifest),
        //   "master")
        //   implicit val timeout = Timeout(5.seconds)
        //   master ? StartServer()
        //   actorSystem.awaitTermination
         s"within doQuery"
         1
        } getOrElse {
          s"Invalid engine ID or version. Aborting query."
          0
        }
      } getOrElse {
        s"Invalid engine instance ID. Aborting query."
        0
      }

	}

  def fetchEngineDataThenQuery(
  	qc: QueryConfig,
  	engineInstance: EngineInstance,
  	engineFactoryName: String,
  	manifest: EngineManifest): String = {

  	val (engineLanguage, engineFactory) = WorkflowUtils.getEngine(engineFactoryName, getClass.getClassLoader)
  	val engine = engineFactory()

  	if (!engine.isInstanceOf[Engine[_,_,_,_,_,_]]) {
  		throw new NoSuchMethodException(s"Engine $engine is not deployable")
  	}

  	val deployableEngine = engine.asInstanceOf[Engine[_,_,_,_,_,_]]

  	runQueryWithEngine(qc, engineInstance, deployableEngine, engineLanguage, manifest)
  }


  def runQueryWithEngine[TD, EIN, PD, Q, P, A](
  	qc: QueryConfig,
  	engineInstance: EngineInstance,
  	engine: Engine[TD, EIN, PD, Q, P, A],
  	engineLanguage: EngineLanguage.Value,
  	manifest: EngineManifest): String = {
  	val engineParams = engine.engineInstanceToEngineParams(engineInstance, qc.jsonExtractor)

  	val kryo = SQKryoInstantiator.newKryoInjection

  	val modelsFromEngineInstance = 
  		kryo.invert(modeldata.get(engineInstance.id).get.models).get.asInstanceOf[Seq[Any]]

  	val batch = if (engineInstance.batch.nonEmpty) {
  		s"${engineInstance.engineFactory} (${engineInstance.batch})"
  	}else{
  		engineInstance.engineFactory
  	}

  	val sparkContext = WorkflowContext(
  		batch = batch,
  		executorEnv = engineInstance.env,
  		mode = "Serving",
  		sparkEnv = engineInstance.sparkConf)

  	val models = engine.prepareDeploy(
  		sparkContext,
  		engineParams,
  		engineInstance.id,
  		modelsFromEngineInstance,
  		params = WorkflowParams()
  	)

  	val algorithms = engineParams.algorithmParamsList.map { case (n, p) =>
  		Doer(engine.algorithmClassMap(n), p)
  	}

  	val servingParamsWithName = engineParams.servingParams

  	val serving = Doer(engine.servingClassMap(servingParamsWithName._1),
  		servingParamsWithName._2)

  	predict[Q, P](
  		qc,
  		engineInstance,
  		engine,
  		engineLanguage,
  		manifest,
  		engineParams.dataSourceParams._2,
  		engineParams.preparatorParams._2,
  		algorithms,
  		engineParams.algorithmParamsList.map(_._2),
  		models,
  		serving,
  		engineParams.servingParams._2,
  		"{\"attr0\": 0, \"attr1\": 1, \"attr2\": 1}")
  }
  def predict[Q, P](
  	args: QueryConfig,
  	engineInstance: EngineInstance,
  	engine: Engine[_,_,_,Q,P,_],
  	engineLanguage: EngineLanguage.Value,
  	manifest: EngineManifest,
  	dataSourceParams: Params,
  	preparatorParams: Params,
  	algorithms: Seq[BaseAlgorithm[_, _, Q, P]],
 	algorithmParams: Seq[Params],
  	models: Seq[Any],
  	serving: BaseServing[Q, P],
  	servingParams: Params,
  	queryString: String): String = {

  	var requestCount: Int = 0
  	var avgServingSec: Double = 0.0
  	var lastServingSec: Double = 0.0

  	val feedbackEnabled = if (args.feedback) {
  		if (args.accessKey.isEmpty) {
  			System.out.println("Feedback loop cannot be enabled because accessKey is empty.")
  			false
  		}else{
  			true
  		}
  	} else false

  	val servingStartTime = DateTime.now
  	val jsonExtractorOption = args.jsonExtractor
  	val queryTime = DateTime.now

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

  	val supplementedQuery = serving.supplementBase(query)

  	val predictions = algorithms.zipWithIndex.map {case (a, ai) =>
  		a.predictBase(models(ai), supplementedQuery)
  	}

  	val prediction = serving.serveBase(query, predictions)
  	val predictionJValue = JsonExtractor.toJValue(
  		jsonExtractorOption,
  		prediction,
  		algorithms.head.querySerializer,
  		algorithms.head.gsonTypeAdapterFactories)

  	val result = if(feedbackEnabled) {
  		implicit val formats = 
  			algorithms.headOption map { alg =>
  				alg.querySerializer
  			} getOrElse {
  				Utils.json4sDefaultFormats
  			}
  		def genPrId: String = Random.alphanumeric.take(64).mkString
  		val newPrId = prediction match {
  			case id: WithPrId =>
  				val org = id.prId
  				if (org.isEmpty) genPrId else org
  			case _ => genPrId
  		}

  		val queryPrId = 
  			query match {
  				case id: WithPrId =>
  					Map("prId" -> id.prId)
  				case _ =>
  					Map()
  			}
  		val data = Map(
  			"event" -> "predict",
  			"eventTime" -> queryTime.toString(),
  			"entityType" -> "pio_pr",
  			"entityId" -> newPrId,
  			"properties" -> Map(
  				"engineInstanceid" -> engineInstance.id,
  				"query" -> query,
  				"prediction" -> prediction)) ++ queryPrId

  		val accessKey = args.accessKey.getOrElse("")
  		val f: Future[Int] = future {
  			scalaj.http.Http(
  				s"http://${args.eventServerIp}:${args.eventServerPort}/" +
  				s"events.json?accessKey=$accessKey").postData(
  				write(data)).header(
  				"content-type", "application/json").asString.code
  		}

 		f onComplete {
 			case Success(code) => {
 				if (code != 201) {
 					System.out.println(s"Feedback event failed. Status code; $code."
 						+ s"Data: ${write{data}}")
 				}
 			}
 			case Failure(t) => {
 				System.out.println(s"Feedback event failed; ${t.getMessage}")
 			}
 		}

 		if (prediction.isInstanceOf[WithPrId]) {
 			predictionJValue merge parse(s"""{"prId" : "$newPrId"}""")
 		} else {
 			predictionJValue
 		}
  	} else predictionJValue

  	val servingEndTime = DateTime.now
  	lastServingSec = 
  		(servingEndTime.getMillis - servingStartTime.getMillis) / 1000.0
  	avgServingSec = 
  		((avgServingSec * requestCount) + lastServingSec) / (requestCount + 1)
  	requestCount += 1

  	compact(render(predictionJValue))
  }

}