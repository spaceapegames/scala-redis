package com.redis.sentinel

import com.redis.RedisNode

class SentinelCluster extends SentinelListener{
  private var sentinelMonitors = Map.empty[SentinelAddress, SentinelMonitor]
  private var listeners = Map.empty[String, SentinelMonitoredRedisMaster]

  def onUpdateSentinels(sentinels: Set[SentinelAddress]){
    sentinelMonitors = sentinels.foldLeft(Map.empty[SentinelAddress, SentinelMonitor]){
      case (m, addr) =>
        m + (addr -> sentinelMonitors.getOrElse(addr, new SentinelMonitor(addr, this)))
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
  def onMastersHeartBeat (values: List[Map[String, String]]) {
    values.map(RedisNode(_)).foreach {
      redisNode =>
        listeners.get(redisNode.name).foreach(_.onMasterHeartBeat(redisNode))
    }
  }

  def getMasterNode(masterName: String): Option[RedisNode] = {
    sentinelMonitors.values.view.map {
      monitor =>
        try {
          //TODO timeout by sentinel request
          monitor.sentinel.master(masterName)
        }catch {
          case e: Exception =>
            None
        }
    }.find(_.isDefined).getOrElse(None).map(values => {RedisNode(values)})
  }
}