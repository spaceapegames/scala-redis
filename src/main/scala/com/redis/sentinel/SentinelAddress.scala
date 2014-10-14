package com.redis.sentinel

object SentinelAddress {
  def apply (addr: String): SentinelAddress = {
    val tokens = addr.split(":")
    SentinelAddress(tokens(0), tokens(1).toInt)
  }
}

case class SentinelAddress (host: String, port: Int) {

}
