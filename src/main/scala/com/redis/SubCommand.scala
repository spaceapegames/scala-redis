package com.redis

trait SubCommand { self: Redis =>
  var pubSub: Boolean = _

  def pSubscribe(channel: String, channels: String*)(fn: PubSubMessage => Any) {
    if (pubSub == true) { // already pubsub ing
      pSubscribeRaw(channel, channels: _*)
      return
    }
    pubSub = true
    pSubscribeRaw(channel, channels: _*)
    new SubscribingThread(this, fn).start
  }

  def pSubscribeRaw(channel: String, channels: String*) {
    send("PSUBSCRIBE", channel :: channels.toList)(())
  }

  def pUnsubscribe = {
    send("PUNSUBSCRIBE")(())
  }

  def pUnsubscribe(channel: String, channels: String*) = {
    send("PUNSUBSCRIBE", channel :: channels.toList)(())
  }
  def subscribe(channel: String, channels: String*)(fn: PubSubMessage => Any) {
    if (pubSub == true) { // already pubsub ing
      subscribeRaw(channel, channels: _*)
    } else {
      pubSub = true
      subscribeRaw(channel, channels: _*)
      new SubscribingThread(this, fn).start
    }
  }

  def subscribeRaw(channel: String, channels: String*) {
    send("SUBSCRIBE", channel :: channels.toList)(())
  }

  def unsubscribe = {
    send("UNSUBSCRIBE")(())
  }

  def unsubscribe(channel: String, channels: String*) = {
    send("UNSUBSCRIBE", channel :: channels.toList)(())
  }


}
