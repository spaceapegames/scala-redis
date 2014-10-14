package com.redis.sentinel

object SentinelClusterBuilder {
  def apply(config: SentinelClusterConfig, addrs: Set[SentinelAddress]): SentinelCluster = {
    val sentinelCluster = new SentinelCluster(config)
    sentinelCluster.onUpdateSentinels(addrs)
    sentinelCluster
  }

  def apply(config: SentinelClusterConfig, autoDiscoveryAddress: SentinelAddress, masterNames: Set[String]): SentinelCluster = {
    val sentinelCluster = new SentinelCluster(config)
    val sentinelAutoDiscoveryClient = new SentinelClient(autoDiscoveryAddress)
    var discoveredAddresses = Set.empty[SentinelAddress]

    var cachedClient = Map.empty[SentinelAddress, SentinelClient]

    masterNames.foreach {
      masterName =>
        val addresses = sentinelAutoDiscoveryClient.sentinelsByMaster(masterName).toSet
        discoveredAddresses ++= addresses

        cachedClient.find( entry => {addresses.contains(entry._1)}) match {
          case Some(entry) =>
            discoveredAddresses ++= entry._2.sentinelsByMaster(masterName).toSet
          case None =>
            val headAddr = addresses.head
            val cache = new SentinelClient(headAddr)
            cachedClient += (headAddr -> cache)
            discoveredAddresses ++= cache.sentinelsByMaster(masterName).toSet
        }
    }
    sentinelCluster.onUpdateSentinels(discoveredAddresses)
    sentinelCluster
  }
}
