package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi
import org.json4s._

/** :: DeveloperApi ::
  * Provides a way to discover clients by ID in a distributed
  * environment
  *
  * @param id Unique identifier of a client.
  * @param url the url of the client.
  */
@DeveloperApi
case class ClientManifest(
  id: String,
  clientId: String,
  url: String)

/** :: DeveloperApi ::
  * Base trait of the [[EngineManifest]] data access object
  *
  * @group Meta Data
  */
@DeveloperApi
trait ClientManifests {
  /** Inserts an [[ClientManifest]] */
  def insert(clientManifest: ClientManifest): Unit

  /** Get an [[ClientManifest]] by its ID */
  def get(clientId: String): Option[ClientManifest]

  /** Updates an [[ClientManifest]] */
  def update(clientInfo: ClientManifest): Unit

  /** Delete an [[ClientManifest]] by its ID */
  def delete(clientId: String): Unit
}

/** :: DeveloperApi ::
  * JSON4S serializer for [[ClientManifest]]
  *
  * @group Meta Data
  */
@DeveloperApi
class ClientManifestSerializer
    extends CustomSerializer[ClientManifest](format => (
  {
    case JObject(fields) =>
      val seed = ClientManifest(
        id = "",
        clientId = "",
        url = "")
      fields.foldLeft(seed) { case (clientManifest, field) =>
        field match {
          case JField("id", JString(id)) => clientManifest.copy(id = id)
          case JField("url", JString(url)) =>
            clientManifest.copy(url = url)
          case _ => clientManifest
        }
      }
  },
  {
    case clientManifest: ClientManifest =>
      JObject(
        JField("id", JString(clientManifest.id)) ::
        JField("clientId", JString(clientManifest.clientId)) ::
        JField("url", JString(clientManifest.url)) ::
        Nil)
  }
))