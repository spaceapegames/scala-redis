package com.redis.sentinel

import com.redis.{SubCommand, Redis}

class SentinelClient(val host: String, val port: Int) extends Redis with SentinelOperations with SubCommand{
  def this(addr: SentinelAddress){
    this(addr.host, addr.port)
  }
}
