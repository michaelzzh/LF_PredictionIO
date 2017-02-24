package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi

case class EngineData(
	val userName: String,
	val accessKey: String,
	val port: Int = 8000
)