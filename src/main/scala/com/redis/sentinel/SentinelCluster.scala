package com.redis.sentinel

import com.redis.{Log, RedisNode}
import scala.collection._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class SentinelCluster (clusterConfig: SentinelClusterConfig = SentinelClusterConfig()) extends SentinelListener with Log{
  private var sentinelMonitors = immutable.Map.empty[SentinelAddress, SentinelMonitor]
  private val listeners: concurrent.Map[String, SentinelMonitoredRedisMaster] = new ConcurrentHashMap[String, SentinelMonitoredRedisMaster]().asScala

  def onUpdateSentinels(sentinels: Set[SentinelAddress]){
    sentinels foreach {
      sentinelAddr =>
        addNewSentinelNode(sentinelAddr)
    }
  }

  def addMonitoredRedisMaster(master: SentinelMonitoredRedisMaster) {
    listeners += (master.getMasterName -> master)
  }

  def removeMonitoredRedisMaster(masterName: String){
    listeners -= masterName
  }

  def onMasterChange (redisNode: RedisNode) {
    listeners.get(redisNode.name).foreach(_.onMasterChange(redisNode))
  }
  def onMastersHeartBeat (values: List[immutable.Map[String, String]]) {
    values.map(RedisNode(_)).foreach {
      redisNode =>
        listeners.get(redisNode.name).foreach(_.onMasterHeartBeat(redisNode))
    }
  }

  def getMasterNode(masterName: String): Option[RedisNode] = {
    this.synchronized {
      sentinelMonitors.values.view.map {
        monitor =>
          try {
            if (!monitor.sentinel.connected) {
              monitor.sentinel.reconnect
            }
            //TODO timeout by sentinel request
            monitor.sentinel.master(masterName)
          } catch {
            case e: Exception =>
              warn("failed to get master node %s from sentinel %s:%s", e, masterName, monitor.sentinel.host, monitor.sentinel.port)
              None
          }
      }.find(_.isDefined).getOrElse(None).map(values => {
        RedisNode(values)
      })
    }
  }

  def addNewSentinelNode(addr: SentinelAddress) {
    this.synchronized {
      if (!sentinelMonitors.contains(addr)) {
        try {
          val monitor = new SentinelMonitor(addr, this, clusterConfig)
          sentinelMonitors += (addr -> sentinelMonitors.getOrElse(addr, monitor))
        } catch {
          case e: Throwable =>
            error("failed to start sentinel client at %s:%s", addr.host, addr.port)
        }
      }
    }
  }
  def removeSentinelNode(addr: SentinelAddress) {
    this.synchronized {
      sentinelMonitors.get(addr).foreach {
        sentinel =>
          sentinel.stop
      }
      sentinelMonitors -= addr
    }
  }

  def getStatus: SentinelClusterStatus = {
    this.synchronized {
      SentinelClusterStatus(sentinelMonitors.keySet, listeners.map(entry => {
        (entry._1 -> entry._2.getNode)
      }).toMap)
    }
  }

  def stopCluster {
    this.synchronized {
      sentinelMonitors.values.foreach(_.stop)
      sentinelMonitors = immutable.Map.empty[SentinelAddress, SentinelMonitor]
    }
  }
}
