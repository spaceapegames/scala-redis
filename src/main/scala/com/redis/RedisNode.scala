package com.redis

object RedisNode {
  def apply (values: Map[String, String]): RedisNode = {
    RedisNode(values("name"), values("ip"), values("port").toInt)
  }
}
case class RedisNode (name: String, host: String, port: Int, maxIdle: Int = 8, database: Int = 0, secret: Option[Any] = None) {

}
