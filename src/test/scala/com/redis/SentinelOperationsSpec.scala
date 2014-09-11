package com.redis

import org.scalatest.{OptionValues, BeforeAndAfterAll, BeforeAndAfterEach, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.redis.sentinel.SentinelClient

//@RunWith(classOf[JUnitRunner])
class SentinelOperationsSpec extends FunSpec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with OptionValues {

  val first = ("10.0.10.220", 6379)
  val firstSlave = ("10.0.13.26", 6379)
  val second = ("10.0.10.139", 6379)
  val secondSlave = ("10.0.8.88", 6379)

  val hosts = List(first, firstSlave, second, secondSlave)
  val sentinels = List(("sandbox-sentinel-1.use1a.apelabs.net", 26379), ("sandbox-sentinel-2.use1a.apelabs.net", 26380), ("sandbox-sentinel-1.use1a.apelabs.net", 26380), ("sandbox-sentinel-2.use1a.apelabs.net", 26380))
  val hostClients = hosts map Function.tupled(new RedisClient(_, _))
  val sentinelClients = sentinels map Function.tupled(new SentinelClient(_, _))

  override def beforeAll() {
    hostClients.foreach { client =>
      if (client.port != 6379) {
        client.slaveof("localhost", 6379)
      }
    }

    Thread sleep 10000 // wait for all the syncs to complete
  }

  override def afterAll() {
    hostClients.foreach (_.slaveof())

    Thread sleep 10000
  }

  describe("masters") {
    it("should return all masters") {
      sentinelClients.foreach { client =>
        val masters = client.masters match {
          case Some(m) => m.collect {
            case Some(details) => (details("name"), details("ip"), details("port"))
          }
          case _ => Nil
        }

        masters.toSet should equal (Set(("first",first._1,first._2.toString), ("second",second._1,second._2.toString)))
      }
    }
  }

  describe("slaves") {
    it("should return all slaves") {
      sentinelClients.foreach { client =>
        val slaves = (client.slaves("first") match {
          case Some(s) => s.collect {
            case Some(details) => (details("ip"), details("port").toInt)
          }
          case _ => Nil
        }).sorted

        slaves(0)._1 should equal (firstSlave._1)
        slaves(0)._2 should equal (firstSlave._2)
      }
    }
  }

  describe("get-master-addr-by-name") {
    it("should return master address") {
      sentinelClients.foreach { client =>
        client.getMasterAddrByName("first") should equal (Some((first._1, first._2)))
      }
    }
  }

  describe("master") {
    it("Show the state and info of the specified master") {
      sentinelClients.foreach { client =>
        val masterInfo = client.master("first").get
        masterInfo("ip") should equal (first._1)
        masterInfo("port").toInt should equal (first._2)
      }
    }
  }

  describe("reset") {
    it("should reset one master") {
      sentinelClients.foreach { client =>
        client.reset("first*") should equal (Some(1))
      }

      Thread sleep 10000 // wait for sentinels to pick up slaves again
    }
  }

//  describe("failover") {
//    it("should automatically update master ip in client") {
//      val masterAddr = new SentinelMonitoredMasterAddress(sentinels, "scala-redis-test")
//      val master = new RedisClient(masterAddr)
//
//      val oldPort = master.port
//
//      sentinelClients(0).failover("scala-redis-test")
//      Thread sleep 30000
//
//      master.port should not equal oldPort
//
//      masterAddr.stopMonitoring()
//    }
//
//    it("should automatically update master ip in pool") {
//      val masterAddr = new SentinelMonitoredMasterAddress(sentinels, "scala-redis-test")
//      val pool = new RedisClientPool(masterAddr)
//
//      var oldPort = -1
//      pool.withClient { client =>
//        oldPort = client.port
//      }
//      oldPort should not equal -1
//
//      sentinelClients(0).failover("scala-redis-test")
//      Thread sleep 30000
//
//      pool.withClient { client =>
//        client.port should not equal oldPort
//      }
//
//      masterAddr.stopMonitoring()
//    }
//  }
}
