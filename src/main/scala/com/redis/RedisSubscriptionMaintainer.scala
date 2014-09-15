package com.redis

trait RedisSubscriptionMaintainer extends Log{
  val maxRetry: Int
  val retryInterval: Long

  var channelListeners = Map.empty[String, SubscriptionReceiver]

  private def callback: PubSubMessage => Unit = pubsub => {
    pubsub match {
      case S(channel, no) => {
        info("subscribed to %s and count %s ", channel, no)
        channelListeners.get(channel).foreach (_.onSubscribed)
      }
      case U(channel, no) => {
        error("unexpected unsubscribing from %s and count %s ", channel, no)
        channelListeners.get(channel).foreach {
          receiver =>
            receiver.onUnsubscribed
            if (receiver.unscribingEnabled){
              channelListeners -= channel
            } else {
              resubscribe(channel)
            }
        }
      }
      case M(channel, msg) =>
        try {
          debug("received %s", msg)
          channelListeners.get(channel).foreach(_.onReceived(msg))
        } catch {
          case e: Throwable => error("Failed to process message. [%s]", e, e.getMessage)
        }
      case E(exception) => {
        error("redis is not available. restart redis consumer and reconnect to redis", exception)
        channelListeners.values.foreach(_.onSubscriptionFailure())
        exceptionHandle()
      }
    }
  }

  private def exceptionHandle(){
    var numOfAttempts = 0
    var handlingComplete = false
    while (!handlingComplete) {
      try {
        reconnect
        resubscribeAll
        handlingComplete = true
      } catch {
        case e: Throwable =>
          numOfAttempts += 1
          if (maxRetry > 0 && numOfAttempts > maxRetry) {
            handlingComplete = true
          } else {
            Thread.sleep(retryInterval)
          }
      }
    }
  }

  def subscribe(channel: String, receiver: SubscriptionReceiver){
    channelListeners += (channel -> receiver)
    getRedisSub.subscribe(channel)(callback)
  }

  def resubscribeAll {
    channelListeners.keys.foreach(resubscribe(_))
  }

  def resubscribe(channel: String){
    channelListeners.get(channel).foreach {
      receiver =>
        getRedisSub.subscribe(channel)(callback)
    }
  }

  protected def getRedisSub: SubCommand
  protected def reconnect
}

trait SubscriptionReceiver {
  def unscribingEnabled: Boolean = false
  def onReceived: String => Unit
  def onSubscriptionFailure: () => Unit
  def onSubscribed {}
  def onUnsubscribed {}
}
