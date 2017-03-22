package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi
import org.json4s._

/** :: DeveloperApi ::
  * Provides a way to discover historic queries by queryIds
  *
  *@param id Unique identifier of a query
  *@param status query status flag
  *@param query the query string
  *@param result the query result
  */
@DeveloperApi
case class QueryHistory( 
	id: String,
	status: String,
	query: String,
	result: String)

@DeveloperApi
trait QueryHistories {
	def insert(queryHistory: QueryHistory): Unit

	def get(id: String): Option[QueryHistory]

	def update(queryInfo: QueryHistory): Unit

	def delete(id: String): Unit
}

@DeveloperApi
class QueryHistorySerializer
	extends CustomSerializer[QueryHistory](format => (
	{
		case JObject(fields) =>
			val seed = QueryHistory(
				id = "",
				status = "",
				query = "",
				result = "")
			fields.foldLeft(seed) {case (queryHistory, field) =>
				field match {
					case JField("id", JString(id)) => queryHistory.copy(id = id)
					case JField("status", JString(status)) => queryHistory.copy(status = status)
					case JField("query", JString(query)) => queryHistory.copy(query = query)
					case JField("result", JString(result)) => queryHistory.copy(result = result)
					case _ => queryHistory
				}
			}
	},
	{
		case queryHistory: QueryHistory =>
			JObject(
				JField("id", JString(queryHistory.id)) ::
				JField("status", JString(queryHistory.status)) ::
				JField("query", JString(queryHistory.query)) ::
				JField("result", JString(queryHistory.result)) ::
				Nil)
	}
))