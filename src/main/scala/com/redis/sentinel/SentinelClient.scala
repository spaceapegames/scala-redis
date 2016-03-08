package com.redis.sentinel

import com.redis.{ConnectionCommand, SubCommand, Redis}

class SentinelClient(val host: String, val port: Int, override val timeout: Int = 0, override val connectionTimeout: Int = 0) extends Redis with ConnectionCommand with SentinelOperations with SubCommand{
  connect

  def this(addr: SentinelAddress){
    this(addr.host, addr.port)
  }
}
