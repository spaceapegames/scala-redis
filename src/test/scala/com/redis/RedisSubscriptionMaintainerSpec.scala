package com.redis

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class RedisSubscriptionMaintainerSpec extends FunSpec
with ShouldMatchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with Log{
  val r = new RedisClient("localhost", 6379)
  val pub = new RedisClient("localhost", 6379)

  val underTest = new RedisSubscriptionMaintainer(){
    val maxRetry: Int = -1
    val retryInterval: Long = 1000
    protected def getRedisSub: SubCommand = r
    protected def reconnect = {
      r.reconnect
    }
  }

  override def beforeEach = r.flushdb

  override def afterEach = r.flushdb

  describe("sub maintainer") {
    it("should auto resubscribe") {
      val receivedA = Promise[Boolean]
      val failureA = Promise[Boolean]
      underTest.subscribe("a", new SubscriptionReceiver {
        override def onSubscriptionFailure: () => Unit = () => failureA.success(true)
        override def onReceived: (String) => Unit = msg => {
          receivedA.success(true)
        }
      })

      val receivedB = Promise[Boolean]
      val failureB = Promise[Boolean]
      val unsubscribeB = Promise[Boolean]
      val subscribeB = Promise[Boolean]

      var counter = 0
      val bSubscribingStatus = subscribeB.future
      val bUnsubscribingStatus = unsubscribeB.future
      underTest.subscribe("b", new SubscriptionReceiver {
        override def onSubscriptionFailure: () => Unit = () => failureB.success(true)
        override def onReceived: (String) => Unit = msg => {
          receivedB.success(true)
        }
        override def onSubscribed{
          counter += 1
          if (counter > 1){
            subscribeB.success(true)
          }
        }
        override def onUnsubscribed{
          unsubscribeB.success(true)
        }
      })

      val receivedAFuture = receivedA.future
      pub.publish("a", "m")
      Await.result(receivedAFuture, 1 second) should be (true)

      val receivedBFuture = receivedB.future
      r.unsubscribe("b")

      Await.result(bUnsubscribingStatus, 50 second) should be (true)
      Await.result(bSubscribingStatus, 50 second) should be (true)

      info("publishing b")
      pub.publish("b", "m")
      info("published b")
      Await.result(receivedBFuture, 3 second) should be (true)
    }
  }
}
