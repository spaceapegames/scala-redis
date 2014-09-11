package com.redis.sentinel

import com.redis._
import com.redis.S
import com.redis.M
import com.redis.U
import scala.concurrent.duration._

class SentinelMonitor (address: SentinelAddress, listener: SentinelListener, config: SentinelClusterConfig) extends Log{
  private var restartCount: Int = 0

  private[sentinel] var sentinel: SentinelClient = _
  private[sentinel] var sentinelSubscriber: SentinelClient = _
  private var hearthBeater: SentinelHearthBeater = _

  init

  private def init {
    sentinelSubscriber = new SentinelClient(address)
    sentinelSubscriber.subscribe("+switch-master")(callback)

    sentinel = new SentinelClient(address)
    if (config.hearthBeatEnabled) {
      hearthBeater = new SentinelHearthBeater {
        def sentinelClient: SentinelClient = new SentinelClient(address)
        def heartBeatListener: SentinelListener = listener
        def hearthBeatInterval: Int = config.hearthBeatInterval
      }
      new Thread(hearthBeater).start()
    }
  }

  def callback: PubSubMessage => Unit = pubsub => pubsub match {
    case S(channel, no) => {
      info("subscribed to %s and count %s ", channel, no)
      restartCount = 0
    }
    case U(channel, no) => {
      error("unexpected unsubscribing from %s and count %s ", channel, no)
    }
    case M(channel, msg) =>
      try {
        debug("received %s", msg)
        val switchMasterMsgs = msg.split(" ")
        if (switchMasterMsgs.size > 3) {
          val (masterName, host, port): (String, String, Int) = try {
            (switchMasterMsgs(0), switchMasterMsgs(3), switchMasterMsgs(4).toInt)
          }
          catch {
            case e: Throwable =>
              error("Message with wrong format received on Sentinel. %s", e, msg)
              (null, null, -1)
          }
          if (masterName != null) {
            listener.onMasterChange(RedisNode(masterName, host, port))
          }
        } else {
          error("Invalid message received on Sentinel. %s", msg)
        }
        //listener
      } catch {
        case e: Throwable => error("Failed to process message. [%s]", e, e.getMessage)
      }
    case E(exception) => {
      error("sentinel is not available. restart sentinel consumer and reconnect to sentinel", exception)
      listener.subscriptionFailure
      autoReconnect
    }
  }

  private def autoReconnect() {
    restartCount = restartCount + 1

    try{
      reconnectSentinel
    } catch {
      case e: Throwable => {
        val retryInterval = (restartCount second).toMillis
        error("failed to reconnect. retrying after %s", retryInterval)
        Thread.sleep(retryInterval)
        autoReconnect
      }
    }
  }

  def reconnectSentinel {
    val newSentinel = new SentinelClient(address)
    try {
      sentinel.disconnect
    } catch {
      case e: Throwable => error("failed to disconnect sentinal for reconnecting")
    }
    sentinel = newSentinel
    sentinel.subscribe("+switch-master")(callback)
  }

  def stop {
    hearthBeater.stop
    sentinel.disconnect
  }
}

trait SentinelHearthBeater extends Runnable with Log{
  private var running = false
  def sentinelClient: SentinelClient
  def heartBeatListener: SentinelListener
  def hearthBeatInterval: Int

  def stop {
    running = false
  }
  def run {
    running = true

    while (running) {
      Thread.sleep(hearthBeatInterval)
      try {
        if (!sentinelClient.connected){
          sentinelClient.reconnect
        }
        sentinelClient.masters match {
          case Some(list) =>
            ifDebug("heart beating on " + sentinelClient.host + ":" + sentinelClient.port)
            heartBeatListener.onMastersHeartBeat(list.filter(_.isDefined).map(_.get))
          case None =>
            ifDebug("heart beat failure")
            heartBeatListener.hearthBeatFailure
        }
      }catch {
        case e: Throwable =>
          ifDebug("heart beat is stopped")
          if (running){
            error("sentinel heart beat failure %s:%s", e, sentinelClient.host, sentinelClient.port)
            heartBeatListener.hearthBeatFailure
          }
      }
    }
  }
}
