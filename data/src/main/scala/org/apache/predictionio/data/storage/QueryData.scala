package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi

case class QueryData(
	val clientId: String,
	val engineId: String,
	val groupId: String,
	val properties: List[QueryEntry] = List()
)

case class QueryEntry(
	val queryId: String,
	val queryString: String
)

case class ResultEntry(
	val queryId: String,
	val resultString: String
)