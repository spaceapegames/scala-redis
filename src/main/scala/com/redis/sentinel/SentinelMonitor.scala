package com.redis.sentinel

import com.redis._
import scala.concurrent.duration._

class SentinelMonitor (address: SentinelAddress, listener: SentinelListener, config: SentinelClusterConfig)
  extends RedisSubscriptionMaintainer
  with Log{

  val maxRetry: Int = -1
  val retryInterval: Long = (2 seconds).toMillis

  private[sentinel] var sentinel: SentinelClient = _
  private[sentinel] var sentinelSubscriber: SentinelClient = _
  private var hearthBeater: SentinelHearthBeater = _
  private val switchMasterListener = new SubscriptionReceiver() {
    def received: String => Unit = msg => {
      onSwitchMaster(msg)
    }
    def subscriptionFailure: () => Unit = () => {
      listener.subscriptionFailure
    }
  }

  init

  private def init {
    sentinelSubscriber = new SentinelClient(address)
    this.subscribe("+switch-master", switchMasterListener)

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

  protected def getRedisSub: SubCommand = sentinelSubscriber
  protected def reconnect {
    val newSentinel = new SentinelClient(address)
    try {
      sentinelSubscriber.disconnect
    } catch {
      case e: Throwable => error("failed to disconnect sentinel for reconnecting")
    }
    sentinelSubscriber = newSentinel
  }

  private def onSwitchMaster(msg: String) {
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
  }

  def stop {
    hearthBeater.stop
    sentinel.disconnect
    sentinelSubscriber.disconnect
  }
}

trait SentinelHearthBeater extends Runnable with Log{
  private var running = false
  def sentinelClient: SentinelClient
  def heartBeatListener: SentinelListener
  def hearthBeatInterval: Int

  def stop {
    running = false
    sentinelClient.disconnect
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
