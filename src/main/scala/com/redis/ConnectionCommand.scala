package com.redis

import com.redis.serialization.{Parse, Format}

trait ConnectionCommand { self: Redis =>
  // QUIT
  // exits the server.
  def quit: Boolean =
    send("QUIT")(disconnect)

  // AUTH
  // auths with the server.
  def auth(secret: Any)(implicit format: Format): Boolean =
    send("AUTH", List(secret))(asBoolean)

  def ping: Option[String] =
    send("PING")(asString)

  def echo[A](data: String)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("ECHO", List(data))(asBulk)

  // SELECT (index)
  // selects the DB to connect, defaults to 0 (zero).
  def select(index: Int): Boolean =
    send("SELECT", List(index))(asBoolean match {
      case true => {
        db = index
        true
      }
      case _ => false
    })
}
