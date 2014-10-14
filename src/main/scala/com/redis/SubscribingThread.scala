package com.redis

class SubscribingThread(redis: Redis, fn: PubSubMessage => Any, threadEnded: () => Unit) extends Thread with Log{
  private var running: Boolean = false
  private var stopped: Boolean = false

  def stopSubscribing {
    running = false
    stopped = true
  }

  override def run {
    try {
      running = true
      while (running) {
        redis.asList match {
          case Some(Some(msgType) :: Some(channel) :: Some(data) :: Nil) =>
            msgType match {
              case "subscribe" | "psubscribe" => fn(S(channel, data.toInt))
              case "unsubscribe" if (data.toInt == 0) =>
                fn(U(channel, data.toInt))
                running = false
              case "punsubscribe" if (data.toInt == 0) =>
                fn(U(channel, data.toInt))
                running = false
              case "unsubscribe" | "punsubscribe" =>
                fn(U(channel, data.toInt))
              case "message" | "pmessage" =>
                fn(M(channel, data))
              case x => throw new RuntimeException("unhandled message: " + x)
            }
          case _ =>
            error("None returned in subscription")
        }
      }
    } catch {
      case e: Throwable =>
        error("subscription error", e)
        running = false
        threadEnded()
        if (!stopped) {
          fn(E(e))
        }
    }
  }
}