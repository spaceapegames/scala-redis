package com.redis.sentinel

import com.redis.{ConnectionCommand, SubCommand, Redis}

class SentinelClient(val host: String, val port: Int) extends Redis with ConnectionCommand with SentinelOperations with SubCommand{
  connect

  def this(addr: SentinelAddress){
    this(addr.host, addr.port)
  }
}
