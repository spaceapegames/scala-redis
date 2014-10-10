package com.redis.sentinel

import scala.concurrent.duration._

case class SentinelClusterConfig (heartBeatEnabled: Boolean = true , heartBeatInterval: Int = 1000, sentinelTimeout: Int = 2000, maxSentinelMonitorRetry: Int = -1, sentinelRetryInterval: Long = (2 seconds).toMillis)
