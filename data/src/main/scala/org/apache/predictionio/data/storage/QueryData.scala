package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi
import com.github.nscala_time.time.Imports._


case class QueryData(
	val engineId: String,
	val groupId: String,
	val properties: List[QueryEntry] = List()
)

case class QueryEntry(
	val queryId: Int,
	val queryString: String
)

case class ResultData(
	val groupId: String = "",
	val status: String = "NONE",
	val progress: Double = 0.0,
	val predictions: List[ResultEntry] = List(),
	val finishTime: DateTime = DateTime.now()
)

case class ResultEntry(
	val queryId: Int,
	val resultString: String
)