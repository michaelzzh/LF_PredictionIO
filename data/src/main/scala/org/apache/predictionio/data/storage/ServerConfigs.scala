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
case class ServerConfig( 
	securityKey: String = "")

@DeveloperApi
trait ServerConfigs {
	def insert(serverConfig: ServerConfig): Unit

	def get(): Option[ServerConfig]

	def update(serverConfig: ServerConfig): Unit

	def delete(id: Int): Unit
}

@DeveloperApi
class ServerConfigSerializer
	extends CustomSerializer[ServerConfig](format => (
	{
		case JObject(fields) =>
			val seed = ServerConfig(
				securityKey = ""
				)
			fields.foldLeft(seed) {case (serverConfig, field) =>
				field match {
					case JField("securityKey", JString(securityKey)) => serverConfig.copy(securityKey = securityKey)
					case _ => serverConfig
				}
			}
	},
	{
		case serverConfig: ServerConfig =>
			JObject(
				JField("securityKey", JString(serverConfig.securityKey)) ::
				Nil)
	}
))