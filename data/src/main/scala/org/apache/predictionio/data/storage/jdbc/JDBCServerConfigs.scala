/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.predictionio.data.storage.jdbc

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.ServerConfig
import org.apache.predictionio.data.storage.ServerConfigs
import org.apache.predictionio.data.storage.StorageClientConfig
import scalikejdbc._

/** JDBC implementation of [[EngineInstances]] */
class JDBCServerConfigs(client: String, config: StorageClientConfig, prefix: String)
  extends ServerConfigs with Logging {
  /** Database table name for this data access object */
  val tableName = JDBCUtils.prefixTableName(prefix, "serverconfig")
  DB autoCommit { implicit session =>
    sql"""
    create table if not exists $tableName (
      id int not null primary key,
      security_key varchar(100)
      )""".execute().apply()
  }

  def insert(i: ServerConfig) = DB localTx { implicit session =>
    sql"""
    INSERT INTO $tableName VALUES(
      1,
      ${i.securityKey}
    )""".update().apply()
  }

  def get(): Option[ServerConfig] = DB localTx { implicit session =>
    sql"""
    SELECT
      security_key
    FROM $tableName WHERE id = 1""".map(resultToServerConfig).
      single().apply()
  }

  def update(i: ServerConfig): Unit = DB localTx { implicit session =>
    sql"""
    update $tableName set
      security_key = ${i.securityKey}
    where id = 1""".update().apply()
  }

  def delete(id: Int): Unit = DB localTx { implicit session =>
    sql"DELETE FROM $tableName WHERE id = 1".update().apply()
  }

  /** Convert JDBC results to [[ServerConfig]] */
  def resultToServerConfig(rs: WrappedResultSet): ServerConfig = {
    ServerConfig(
      securityKey = rs.string("security_key")
    )
  }
}
