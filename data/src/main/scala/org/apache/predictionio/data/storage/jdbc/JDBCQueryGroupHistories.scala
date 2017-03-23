package org.apache.predictionio.data.storage.jdbc

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.QueryGroupHistories
import org.apache.predictionio.data.storage.QueryGroupHistory
import org.apache.predictionio.data.storage.StorageClientConfig
import scalikejdbc._

/** JDBC implementation of [[ClientManifests]] */
class JDBCQueryGroupHistories(client: String, config: StorageClientConfig, prefix: String)
  extends QueryGroupHistories with Logging {
  /** Database table name for this data access object */
  val tableName = JDBCUtils.prefixTableName(prefix, "querygrouphistories")
  DB autoCommit { implicit session =>
    sql"""
    create table if not exists $tableName (
      groupid varchar(100) not null primary key,
      engineid text not null,
      status text not null,
      progress real not null)""".execute().apply()
  }

  def insert(i: QueryGroupHistory): Unit = DB localTx { implicit session =>
    sql"""
    INSERT INTO $tableName VALUES(
      ${i.groupId},
      ${i.engineId},
      ${i.status},
      ${i.progress})""".update().apply()
  }

  def get(groupId: String, engineId: String): Option[QueryGroupHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      groupid,
      engineid,
      status,
      progress
    FROM $tableName WHERE id = $groupId AND engineid = $engineId""".map(resultToQueryGroupHistory).single().apply()
  }

  def getCompleted(groupId: String, engineId: String): Option[QueryGroupHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      groupid,
      engineid,
      status,
      progress
    FROM $tableName
    WHERE
      groupid = $groupId AND 
      engineid = $engineId AND
      status = 'COMPLETED'""".map(resultToQueryGroupHistory).single().apply()
  }

  def update(i: QueryGroupHistory): Unit = DB localTx { implicit session =>
    sql"""
    update $tableName set
      status = ${i.status},
      progress = ${i.progress}
    where groupid = ${i.groupId} AND engineid = ${i.engineId}""".update().apply()
  }

  def delete(groupId: String, engineId: String): Unit = DB localTx { implicit session =>
    sql"DELETE FROM $tableName WHERE groupid = $groupId AND engineid = $engineId".update().apply()
  }

  /** Convert JDBC results to [[ClientManfiest]] */
  def resultToQueryGroupHistory(rs: WrappedResultSet): QueryGroupHistory = {
    QueryGroupHistory(
      groupId = rs.string("groupid"),
      engineId = rs.string("engineid"),
      status = rs.string("status"),
      progress = rs.double("progress"))
  }
}