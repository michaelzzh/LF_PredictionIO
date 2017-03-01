package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi

case class QueryData(
	val userName: String,
	val port: Int = 8000,
	val query: String
)