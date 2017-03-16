package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi

case class QueryData(
	val clientId: String,
	val engineId: String,
	val properties: List[String] = List()
)