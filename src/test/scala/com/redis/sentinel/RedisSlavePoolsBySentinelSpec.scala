package com.redis.sentinel

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import com.redis.RedisNode

@RunWith(classOf[JUnitRunner])
class RedisSlavePoolsBySentinelSpec  extends FunSpec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  describe("latest slaves") {
    it("should merge with current slaves") {
      var counter = 0
      val sentinelCluster = new SentinelCluster(){
        override def getSlaves(masterName: String): List[RedisNode] = {
          if (counter == 0){
            List(RedisNode("localhost:6379", "localhost", 6379), RedisNode("localhost:6381", "localhost", 6381))
          } else
            List(RedisNode("localhost:6379", "localhost", 6379), RedisNode("localhost:6380", "localhost", 6380))
        }
      }
      val pool = new RedisSlavePoolsBySentinel("test-service", sentinelCluster)
      pool.startSlaveSync(100)

      var list = List.empty[RedisNode]
      for (i <- 0 until 8){
        list = list.::(pool.getNextSlave.getNode)
      }
      list.filter(_.name == "localhost:6379").size should equal (4)
      list.filter(_.name == "localhost:6381").size should equal (4)

      counter += 1
      Thread.sleep(500)

      list = List.empty[RedisNode]
      for (i <- 0 until 8){
        list = list.::(pool.getNextSlave.getNode)
      }

      list.filter(_.name == "localhost:6379").size should equal (4)
      list.filter(_.name == "localhost:6381").size should equal (0)
      list.filter(_.name == "localhost:6380").size should equal (4)
    }
  }
}
