package org.apache.predictionio.data.storage.jdbc

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.QueryHistories
import org.apache.predictionio.data.storage.QueryHistory
import org.apache.predictionio.data.storage.StorageClientConfig
import scalikejdbc._

/** JDBC implementation of [[ClientManifests]] */
class JDBCQueryHistories(client: String, config: StorageClientConfig, prefix: String)
  extends QueryHistories with Logging {
  /** Database table name for this data access object */
  val tableName = JDBCUtils.prefixTableName(prefix, "queryhistories")
  DB autoCommit { implicit session =>
    sql"""
    create table if not exists $tableName (
      id varchar(100) not null primary key,
      queryid int not null,
      groupid text not null,
      status text not null,
      query text not null,
      result text not null)""".execute().apply()
  }

  def insert(i: QueryHistory): String = DB localTx { implicit session =>
    val id = java.util.UUID.randomUUID().toString
    sql"""
    INSERT INTO $tableName VALUES(
      $id,
      ${i.queryId},
      ${i.groupId},
      ${i.status},
      ${i.query},
      ${i.result})""".update().apply()
    id
  }

  def get(queryId: Int, groupId: String): Option[QueryHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      id,
      queryid,
      groupid,
      status,
      query,
      result
    FROM $tableName 
    WHERE 
      queryid = $queryId AND 
      groupid = $groupId""".map(resultToQueryHistory).single().apply()
  }

  def getWithId(id: String): Option[QueryHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      id,
      queryid,
      groupid,
      status,
      query,
      result
    FROM $tableName
    WHERE
      id = $id""".map(resultToQueryHistory).single().apply()
  }

  def getGroup(groupId: String): List[QueryHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      id,
      queryid,
      groupid,
      status,
      query,
      result
    FROM $tableName 
    WHERE 
      groupid = $groupId AND
      status = 'COMPLETED'
    ORDER BY queryid ASC""".map(resultToQueryHistory).list().apply()
  }

  def update(i: QueryHistory): Unit = DB localTx { implicit session =>
    sql"""
    update $tableName set
      status = ${i.status},
      query = ${i.query},
      result = ${i.result}
    where queryid = ${i.queryId} AND groupid = ${i.groupId}""".update().apply()
  }

  def delete(queryId: Int, groupId: String): Unit = DB localTx { implicit session =>
    sql"DELETE FROM $tableName WHERE queryid = $queryId AND groupid = $groupId".update().apply()
  }

  /** Convert JDBC results to [[ClientManfiest]] */
  def resultToQueryHistory(rs: WrappedResultSet): QueryHistory = {
    QueryHistory(
      id = rs.string("id"),
      queryId = rs.int("queryid"),
      groupId = rs.string("groupid"),
      status = rs.string("status"),
      query = rs.string("query"),
      result = rs.string("result"))
  }
}