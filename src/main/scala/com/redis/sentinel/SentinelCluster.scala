package com.redis.sentinel

import com.redis.{Log, RedisNode}

class SentinelCluster (clusterConfig: SentinelClusterConfig = SentinelClusterConfig()) extends SentinelListener with Log{
  private var sentinelMonitors = Map.empty[SentinelAddress, SentinelMonitor]
  private var listeners = Map.empty[String, SentinelMonitoredRedisMaster]

  def onUpdateSentinels(sentinels: Set[SentinelAddress]){
    sentinelMonitors = sentinels.foldLeft(Map.empty[SentinelAddress, SentinelMonitor]){
      case (m, addr) =>
        m + (addr -> sentinelMonitors.getOrElse(addr, new SentinelMonitor(addr, this, clusterConfig)))
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
          if (!monitor.sentinel.connected){
            monitor.sentinel.reconnect
          }
          //TODO timeout by sentinel request
          monitor.sentinel.master(masterName)
        }catch {
          case e: Exception =>
            warn("failed to get master node %s from sentinel %s:%s", e, masterName, monitor.sentinel.host, monitor.sentinel.port)
            None
        }
    }.find(_.isDefined).getOrElse(None).map(values => {RedisNode(values)})
  }
}
