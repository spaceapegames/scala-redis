package com.redis

sealed trait PubSubMessage
case class S(channel: String, noSubscribed: Int) extends PubSubMessage
case class U(channel: String, noSubscribed: Int) extends PubSubMessage
case class M(origChannel: String, message: String) extends PubSubMessage
case class E(e: java.lang.Throwable) extends PubSubMessage