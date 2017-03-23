package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi
import org.json4s._

/** :: DeveloperApi ::
  * Provides a way to discover historic queries by queryIds
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
	progress: Double)

@DeveloperApi
trait QueryGroupHistories {
	def insert(queryHistory: QueryGroupHistory): Unit

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
				progress = 0.0)
			fields.foldLeft(seed) {case (queryGroupHistory, field) =>
				field match {
					case JField("groupId", JString(groupId)) => queryGroupHistory.copy(groupId = groupId)
					case JField("engineId", JString(engineId)) => queryGroupHistory.copy(engineId = engineId)
					case JField("status", JString(status)) => queryGroupHistory.copy(status = status)
					case JField("query", JDouble(progress)) => queryGroupHistory.copy(progress = progress)
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