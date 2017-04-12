package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi
import org.json4s._

/** :: DeveloperApi ::
  * Provides a way to discover historic queries by queryIds
  *
  *@param id Unique identifier of a query
  *@param queryId identifier for each query in a group
  *@param groupId identifier for the group a query belongs to
  *@param status query status flag
  *@param query the query string
  *@param result the query result
  */
@DeveloperApi
case class QueryHistory( 
	id: String,
	queryId: Int,
	groupId: String,
	status: String,
	query: String,
	result: String)

@DeveloperApi
trait QueryHistories {
	def insert(queryHistory: QueryHistory): String

	def get(queryId: Int, groupId: String): Option[QueryHistory]

	def getWithId(id: String): Option[QueryHistory]

	def getGroup(groupId: String): List[QueryHistory]

	def update(queryInfo: QueryHistory): Unit

	def delete(queryId: Int, groupId: String): Unit
}

@DeveloperApi
class QueryHistorySerializer
	extends CustomSerializer[QueryHistory](format => (
	{
		case JObject(fields) =>
			val seed = QueryHistory(
				id = "",
				queryId = 0,
				groupId = "",
				status = "",
				query = "",
				result = "")
			fields.foldLeft(seed) {case (queryHistory, field) =>
				field match {
					case JField("id", JString(id)) => queryHistory.copy(id = id)
					case JField("queryId", JInt(queryId)) => queryHistory.copy(queryId = queryId.intValue())
					case JField("groupId", JString(groupId)) => queryHistory.copy(groupId = groupId)
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
				JField("queryId", JInt(queryHistory.queryId)) ::
				JField("groupId", JString(queryHistory.groupId)) ::
				JField("status", JString(queryHistory.status)) ::
				JField("query", JString(queryHistory.query)) ::
				JField("result", JString(queryHistory.result)) ::
				Nil)
	}
))