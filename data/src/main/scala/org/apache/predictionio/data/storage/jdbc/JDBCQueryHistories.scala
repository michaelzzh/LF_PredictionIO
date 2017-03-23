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
      groupid text not null,
      status text not null,
      query text not null,
      result text not null)""".execute().apply()
  }

  def insert(i: QueryHistory): Unit = DB localTx { implicit session =>
    sql"""
    INSERT INTO $tableName VALUES(
      ${i.id},
      ${i.groupId},
      ${i.status},
      ${i.query},
      ${i.result})""".update().apply()
  }

  def get(queryId: String): Option[QueryHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      id,
      groupid,
      status,
      query,
      result
    FROM $tableName WHERE id = $queryId""".map(resultToQueryHistory).single().apply()
  }

  def getGroup(groupId: String): List[QueryHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      id,
      groupid,
      status,
      query,
      result
    FROM $tableName 
    WHERE 
      groupid = $groupId AND
      status = 'COMPLETED'""".map(resultToQueryHistory).list().apply()
  }

  def update(i: QueryHistory): Unit = DB localTx { implicit session =>
    sql"""
    update $tableName set
      groupid = ${i.groupId},
      status = ${i.status},
      query = ${i.query},
      result = ${i.result}
    where id = ${i.id}""".update().apply()
  }

  def delete(id: String): Unit = DB localTx { implicit session =>
    sql"DELETE FROM $tableName WHERE id = $id".update().apply()
  }

  /** Convert JDBC results to [[ClientManfiest]] */
  def resultToQueryHistory(rs: WrappedResultSet): QueryHistory = {
    QueryHistory(
      id = rs.string("id"),
      groupId = rs.string("groupid"),
      status = rs.string("status"),
      query = rs.string("query"),
      result = rs.string("result"))
  }
}