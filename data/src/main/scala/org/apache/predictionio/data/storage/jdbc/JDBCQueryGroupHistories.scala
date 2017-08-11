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
      progress real not null,
      finishtime timestamp without time zone default now())""".execute().apply()
  }

  def insert(i: QueryGroupHistory): String = DB localTx { implicit session =>
    val id = java.util.UUID.randomUUID().toString
    sql"""
    INSERT INTO $tableName VALUES(
      ${id},
      ${i.engineId},
      ${i.status},
      ${i.progress},
      ${i.finishTime})""".update().apply()
    id
  }

  def get(groupId: String, engineId: String): Option[QueryGroupHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      groupid,
      engineid,
      status,
      progress,
      finishtime
    FROM $tableName WHERE groupid = $groupId AND engineid = $engineId""".map(resultToQueryGroupHistory).single().apply()
  }

  def getCompleted(groupId: String, engineId: String): Option[QueryGroupHistory] = DB localTx { implicit session =>
    sql"""
    SELECT
      groupid,
      engineid,
      status,
      progress,
      finishtime
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

  def count(): Option[Int] = DB localTx { implicit session =>
    sql"SELECT count(*) FROM $tableName".map(rs => rs.int("count")).single().apply()
  }

  def deleteSomeOldest(queryHistoriesTableName : SQLSyntax, limit : Int): Unit = DB localTx { implicit session => 
    sql"""
    DELETE FROM $queryHistoriesTableName WHERE groupid IN
    (SELECT groupid FROM $tableName ORDER BY finishtime LIMIT $limit);
    DELETE FROM $tableName
    WHERE ctid IN (
    SELECT ctid
    FROM $tableName
    ORDER BY finishtime
    LIMIT $limit
    )""".update().apply()
  }

  /** Convert JDBC results to [[ClientManfiest]] */
  def resultToQueryGroupHistory(rs: WrappedResultSet): QueryGroupHistory = {
    QueryGroupHistory(
      groupId = rs.string("groupid"),
      engineId = rs.string("engineid"),
      status = rs.string("status"),
      progress = rs.double("progress"),
      finishTime = rs.jodaDateTime("finishtime"))
  }
}