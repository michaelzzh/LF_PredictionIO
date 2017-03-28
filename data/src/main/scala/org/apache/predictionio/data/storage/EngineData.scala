package org.apache.predictionio.data.storage

import org.apache.predictionio.annotation.DeveloperApi

case class EngineData(
	val engineId: String,
	val baseEngine: String
)