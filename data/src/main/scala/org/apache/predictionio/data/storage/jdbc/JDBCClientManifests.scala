package org.apache.predictionio.data.storage.jdbc

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.ClientManifests
import org.apache.predictionio.data.storage.ClientManifest
import org.apache.predictionio.data.storage.StorageClientConfig
import scalikejdbc._

/** JDBC implementation of [[ClientManifests]] */
class JDBCClientManifests(client: String, config: StorageClientConfig, prefix: String)
  extends ClientManifests with Logging {
  /** Database table name for this data access object */
  val tableName = JDBCUtils.prefixTableName(prefix, "clientmanifests")
  DB autoCommit { implicit session =>
    sql"""
    create table if not exists $tableName (
      id varchar(100) not null primary key,
      url text not null)""".execute().apply()
  }

  def insert(i: ClientManifest): Unit = DB localTx { implicit session =>
    sql"""
    INSERT INTO $tableName VALUES(
      ${i.id},
      ${i.url})""".update().apply()
  }

  def get(id: String): Option[ClientManifest] = DB localTx { implicit session =>
    sql"""
    SELECT
      id,
      url
    FROM $tableName WHERE id = $id""".map(resultToClientManifest).single().apply()
  }

  def update(i: ClientManifest): Unit = DB localTx { implicit session =>
    sql"""
    update $tableName set
      url = ${i.url}
    where id = ${i.id}""".update().apply()
  }

  def delete(id: String): Unit = DB localTx { implicit session =>
    sql"DELETE FROM $tableName WHERE id = $id".update().apply()
  }

  /** Convert JDBC results to [[ClientManfiest]] */
  def resultToClientManifest(rs: WrappedResultSet): ClientManifest = {
    ClientManifest(
      id = rs.string("id"),
      url = rs.string("url"))
  }
}