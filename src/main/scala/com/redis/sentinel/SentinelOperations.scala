package com.redis.sentinel

import com.redis.serialization.{Parse, Format}
import com.redis.Redis

trait SentinelOperations { self: Redis =>

  def masters[K,V](implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[List[Option[Map[K,V]]]] =
    send("SENTINEL", List("MASTERS"))(asListOfListPairs[K,V].map(_.map(_.map(_.flatten.toMap))))

  def master[K,V](masterName: String)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[Map[K,V]] = {
    send("SENTINEL", List("MASTER", masterName))(asListPairs[K,V].map(_.flatten.toMap))
  }

  def slaves[K,V](name: String)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]):
  Option[List[Option[Map[K,V]]]] =
    send("SENTINEL", List("SLAVES", name))(asListOfListPairs[K,V].map(_.map(_.map(_.flatten.toMap))))

  def getMasterAddrByName(name: String): Option[(String, Int)] =
    send("SENTINEL", List("GET-MASTER-ADDR-BY-NAME", name))(asList[String]) match {
      case Some(List(Some(h), Some(p))) => Some(h, p.toInt)
      case _ => None
    }

  def reset(pattern: String): Option[Int] =
    send("SENTINEL", List("RESET", pattern))(asInt)

  def failover(name: String): Boolean =
    send("SENTINEL", List("FAILOVER", name))(asBoolean)

  def sentinelsByMaster(masterName: String)(implicit format: Format): List[SentinelAddress] = {
    val addresses =  send("SENTINEL", List("sentinels", masterName))(asListOfListPairs[String, String].map(_.map(_.map(_.flatten.toMap))))  match {
      case Some(m) => m.collect {
        case Some(details) => (details("name"), details("ip"), details("port").toInt)
      }
      case _ => Nil
    }
    addresses.map(v => {SentinelAddress(v._2, v._3)})
  }
}

