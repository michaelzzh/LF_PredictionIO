package org.apache.predictionio.data.storage

import com.github.nscala_time.time.Imports._
import org.apache.predictionio.annotation.DeveloperApi
import org.json4s._

/** :: DeveloperApi ::
  * DataAccess object for queryGroupHistories. Past query groups can be retrieved using groupId and engineId. 
  * 
  *
  *@param id Unique identifier of a query
  *@param status query status flag
  *@param progress group query progress
  */
@DeveloperApi
case class QueryGroupHistory( 
	groupId: String,
	engineId: String,
	status: String,
	progress: Double,
	finishTime: DateTime)

@DeveloperApi
trait QueryGroupHistories {
	def insert(queryHistory: QueryGroupHistory): String

	def get(groupId: String, engineId: String): Option[QueryGroupHistory]

	def getCompleted(groupId: String, engineId: String): Option[QueryGroupHistory]

	def update(queryInfo: QueryGroupHistory): Unit

	def delete(groupId: String, engineId: String): Unit
}

@DeveloperApi
class QueryGroupHistorySerializer
	extends CustomSerializer[QueryGroupHistory](format => (
	{
		case JObject(fields) =>
			val seed = QueryGroupHistory(
				groupId = "",
				engineId = "",
				status = "",
				progress = 0.0,
				startTime = DateTime.now())
			fields.foldLeft(seed) {case (queryGroupHistory, field) =>
				field match {
					case JField("groupId", JString(groupId)) => queryGroupHistory.copy(groupId = groupId)
					case JField("engineId", JString(engineId)) => queryGroupHistory.copy(engineId = engineId)
					case JField("status", JString(status)) => queryGroupHistory.copy(status = status)
					case JField("query", JDouble(progress)) => queryGroupHistory.copy(progress = progress)
					case JField("startTime", JString(finishTime)) => queryGroupHistory.copy(finishTime = Utils.stringToDateTime(finishTime))
					case _ => queryGroupHistory
				}
			}
	},
	{
		case queryGroupHistory: QueryGroupHistory =>
			JObject(
				JField("groupId", JString(queryGroupHistory.groupId)) ::
				JField("status", JString(queryGroupHistory.status)) ::
				JField("progress", JDouble(queryGroupHistory.progress)) ::
				Nil)
	}
))