package com.redis

import java.net.{SocketTimeoutException, ServerSocket}
import java.util.concurrent.CountDownLatch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class IOTimeoutSpec  extends FunSpec with Matchers {


  describe("connection timeout test") {
    it("should timeout because it can't connect") {
      val startTime = System.currentTimeMillis()
      intercept[RedisConnectionException] {
        new IO {
          override val connectionTimeout: Int = 2000
          override val host: String = "10.255.255.1"
          override val timeout: Int = 0
          override val port: Int = 6379
        }.connect
      }

      System.currentTimeMillis()-startTime should be >= 2000l
    }
  }

  describe("socket timeout test") {
    it("should timeout because no data arrived") {
      val socket = new ServerSocket(6378)

      import scala.concurrent.ExecutionContext.Implicits.global

      Future {
        val client = socket.accept()
        println("accepted")
        client.setKeepAlive(false)
      }
      Thread.sleep(100)

      val startTime = System.currentTimeMillis()
      intercept[RedisConnectionException] {
        val ioUnderTest = new IO {
          override val connectionTimeout: Int = 1000
          override val host: String = "localhost"
          override val timeout: Int = 2000
          override val port: Int = 6378
        }
        ioUnderTest.connect
        ioUnderTest.readLine
      }

      System.currentTimeMillis()-startTime should be >= 2000l
    }
  }
}
