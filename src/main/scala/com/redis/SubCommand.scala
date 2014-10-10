package com.redis

trait SubCommand { self: Redis =>
  var pubSub: Boolean = _

  private def startSubscribing(fn: PubSubMessage => Any) {
    //we don't expect huge scale of subscribing requests
    this.synchronized {
      if (pubSub == false) { // already pubsub ing
        pubSub = true
        ifDebug("start a new subscribing thread from redis instance "+this.toString)
        new SubscribingThread(this, fn, subscribingStopped).start()
      }
    }
  }
  private def subscribingStopped(){
    this.synchronized {
      pubSub = false
    }
  }

  def pSubscribe(channel: String, channels: String*)(fn: PubSubMessage => Any) {
    startSubscribing(fn)
    pSubscribeRaw(channel, channels: _*)
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
    startSubscribing(fn)
    subscribeRaw(channel, channels: _*)
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
