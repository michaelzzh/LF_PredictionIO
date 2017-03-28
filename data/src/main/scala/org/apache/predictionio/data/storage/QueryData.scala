package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi

case class QueryData(
	val engineId: String,
	val groupId: String,
	val properties: List[QueryEntry] = List()
)

case class QueryEntry(
	val queryId: String,
	val queryString: String
)

case class ResultData(
	val groupId: String = "",
	val status: String = "EMPTY",
	val progress: Double = 0.0,
	val predictions: List[ResultEntry] = List()
)

case class ResultEntry(
	val queryId: String,
	val resultString: String
)