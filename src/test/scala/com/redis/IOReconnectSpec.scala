package com.redis

import com.redis.serialization.Parse
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

@RunWith(classOf[JUnitRunner])
class IOReconnectSpec extends FunSpec
with Matchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with Eventually
with Log{
  var fakingRedisDown = false
  var disconnectCount = 0
  var reconnected = false
  var subscribed = false
  val r = new RedisClient("localhost", 6379)
  val pub = new RedisClient("localhost", 6379)
  val sub = new RedisClient("localhost", 6379) {
    // asList will get called constantly by the SubscribingThread, so using this as a mechanism for faking Redis going down.
    override def asList[T](implicit parse: Parse[T]): Option[List[Option[T]]] = {
      if (fakingRedisDown) {
        throw new RuntimeException("faking redis being down")
      }
      else {
        super.asList
      }
    }

    override def disconnect: Boolean = {
      disconnectCount = disconnectCount + 1
      super.disconnect
    }

    override def connect: Boolean = {
      if (fakingRedisDown) {
        false
      }
      else {
        val result = super.connect
        reconnected = result
        result
      }
    }

    override def subscribe(channel: String, channels: String*)(fn: PubSubMessage => Any): Unit = {
      super.subscribe(channel)(fn)
      subscribed = true
    }
  }

  val underTest = new RedisSubscriptionMaintainer(){
    val maxRetry: Int = -1
    val retryInterval: Long = 1000
    protected def getRedisSub: SubCommand = sub
    override protected def reconnect: Boolean = {
      sub.reconnect
    }
  }

  override def beforeEach: Unit = r.flushdb

  override def afterEach: Unit = r.flushdb

  describe("redis client") {
    it("should be able to call disconnect multiple times") {

      // this test simulated the behaviour when redis is down.  In that instance, 'reconnect' will get called
      // repeatedly.  reconnect does a disconnect then a connect.
      // if redis is still down, then of course the connect will fail again, causing another disconnect/connect call.
      // In effect, it becomes disconnect, connect, disconnect, connect, ..., connect (success)
      //
      // However, there was a bug where the second call to disconnect would fail, return false, and then the connect would never get called.
      // So despite redis coming back, the subscriber was never able to reconnect.

      val receivedA = Promise[Boolean]
      val failureA = Promise[Boolean]
      underTest.subscribe("a", new SubscriptionReceiver {
        override def onSubscriptionFailure: () => Unit = () => failureA.success(true)
        override def onReceived: (String) => Unit = msg => {
          receivedA.success(true)
        }
      })

      fakingRedisDown = true
      eventually (timeout((underTest.retryInterval*2 + 1).milliseconds)) {
        disconnectCount shouldBe 2
      }

      subscribed = false
      reconnected = false
      fakingRedisDown = false
      eventually (timeout((underTest.retryInterval + 1).milliseconds)) {
        reconnected shouldBe true
      }

      val receivedAFuture = receivedA.future

      eventually (timeout((underTest.retryInterval + 1).milliseconds)) {
        subscribed shouldBe true
        pub.publish("a", "m")
        Await.result(receivedAFuture, 1.second) should be (true)
      }
    }
  }
}
