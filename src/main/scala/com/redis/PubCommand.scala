package com.redis

trait PubCommand { self: Redis =>
  def publish(channel: String, msg: String): Option[Long] = {
    send("PUBLISH", List(channel, msg))(asLong)
  }
}
