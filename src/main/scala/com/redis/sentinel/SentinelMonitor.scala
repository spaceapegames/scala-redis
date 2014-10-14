package com.redis.sentinel

import com.redis._
import scala.concurrent.duration._

class SentinelMonitor (address: SentinelAddress, listener: SentinelListener, config: SentinelClusterConfig)
  extends RedisSubscriptionMaintainer
  with Log{

  val maxRetry: Int = config.maxSentinelMonitorRetry
  val retryInterval: Long = config.sentinelRetryInterval

  private[sentinel] var sentinel: SentinelClient = _
  private[sentinel] var sentinelSubscriber: SentinelClient = _
  private var heartBeater: SentinelHeartBeater = _
  private val switchMasterListener = new SubscriptionReceiver() {
    def onReceived: String => Unit = msg => {
      onSwitchMaster(msg)
    }
    def onSubscriptionFailure: () => Unit = () => {
      listener.subscriptionFailure
    }
  }

  private val newSentinelListener = new SubscriptionReceiver() {
    def onReceived: String => Unit = msg => {
      val tokens = msg.split(" ")
      if (tokens(0) == "sentinel" && tokens.size == 8)
        listener.addNewSentinelNode(SentinelAddress(tokens(1)))
      else
        error("invalid +sentinel message: %s", msg)
    }
    def onSubscriptionFailure: () => Unit = () => {
      listener.subscriptionFailure
    }
  }

  private val downListener = new SubscriptionReceiver() {
    def onReceived: String => Unit = msg => {
      val tokens = msg.split(" ")
      if (tokens(0) == "sentinel" && tokens.size == 8)
        listener.removeSentinelNode(SentinelAddress(tokens(1)))
      else
        warn("unsupported +sdown: %s", msg)
    }
    def onSubscriptionFailure: () => Unit = () => {
      listener.subscriptionFailure
    }
  }

  init

  private def init {
    if (config.sentinelSubscriptionEnabled) {
      sentinelSubscriber = new SentinelClient(address)
      this.subscribe("+switch-master", switchMasterListener)
      this.subscribe("+sentinel", newSentinelListener)
      this.subscribe("+sdown", downListener)
    }

    sentinel = new SentinelClient(address)
    if (config.heartBeatEnabled) {
      heartBeater = new SentinelHeartBeater {
        val sentinelClient: SentinelClient = new SentinelClient(address)
        def heartBeatListener: SentinelListener = listener
        def heartBeatInterval: Int = config.heartBeatInterval
      }
      new Thread(heartBeater).start()
    }
  }

  protected def getRedisSub: SubCommand = sentinelSubscriber
  protected def reconnect: Boolean = {
    try {
      sentinelSubscriber.disconnect
      sentinelSubscriber.connect
    } catch {
      case e: Throwable =>
        error("failed to reconnect sentinel", e)
        false
    }
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
    heartBeater.stop
    sentinel.disconnect
    sentinelSubscriber.stopSubscribing
    sentinelSubscriber.disconnect
  }
}

trait SentinelHeartBeater extends Runnable with Log{
  private var running = false
  val sentinelClient: SentinelClient
  def heartBeatListener: SentinelListener
  def heartBeatInterval: Int

  def stop {
    running = false
  }
  def run {
    running = true

    while (running) {
      Thread.sleep(heartBeatInterval)
      try {
        if (!sentinelClient.connected){
          sentinelClient.disconnect
          sentinelClient.connect
        }
        sentinelClient.masters match {
          case Some(list) =>
            ifDebug("heart beating on " + sentinelClient.host + ":" + sentinelClient.port)
            heartBeatListener.onMastersHeartBeat(list.filter(_.isDefined).map(_.get))
          case None =>
            ifDebug("heart beat failure")
            heartBeatListener.heartBeatFailure
        }
      }catch {
        case e: Throwable =>
          ifDebug("heart beat is failed. running status "+running)
          if (running){
            error("sentinel heart beat failure")
            heartBeatListener.heartBeatFailure
          }
      }
    }
    ifDebug("heart beat is stopped. ")
    sentinelClient.disconnect
  }
}
