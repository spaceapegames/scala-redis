package com.redis

import serialization._

  // SORT
  // sort keys in a set, and optionally pull values for them
trait Operations extends ConnectionCommand{ self: Redis =>
  def sort[A](key:String, limit:Option[Pair[Int, Int]] = None, desc:Boolean = false, alpha:Boolean = false, by:Option[String] = None, get:List[String] = Nil)(implicit format:Format, parse:Parse[A]):Option[List[Option[A]]] = {
    val commands:List[Any] =
      List(List(key), limit.map(l => List("LIMIT", l._1, l._2)).getOrElse(Nil)
      , (if (desc) List("DESC") else Nil)
      , (if (alpha) List("ALPHA") else Nil)
      , by.map(b => List("BY", b)).getOrElse(Nil)
      , get.map(g => List("GET", g)).flatMap(x=>x)
      ).flatMap(x=>x)
    send("SORT", commands)(asList)
  }
  // KEYS
  // returns all the keys matching the glob-style pattern.
  def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    send("KEYS", List(pattern))(asList)

  // RANDKEY
  // return a randomly selected key from the currently selected DB.
  @deprecated("use randomkey", "2.8") def randkey[A](implicit parse: Parse[A]): Option[A] =
    send("RANDOMKEY")(asBulk)

  // RANDOMKEY
  // return a randomly selected key from the currently selected DB.
  def randomkey[A](implicit parse: Parse[A]): Option[A] =
    send("RANDOMKEY")(asBulk)

  // RENAME (oldkey, newkey)
  // atomically renames the key oldkey to newkey.
  def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean =
    send("RENAME", List(oldkey, newkey))(asBoolean)
  
  // RENAMENX (oldkey, newkey)
  // rename oldkey into newkey but fails if the destination key newkey already exists.
  def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean =
    send("RENAMENX", List(oldkey, newkey))(asBoolean)
  
  // DBSIZE
  // return the size of the db.
  def dbsize: Option[Long] =
    send("DBSIZE")(asLong)

  // EXISTS (key)
  // test if the specified key exists.
  def exists(key: Any)(implicit format: Format): Boolean =
    send("EXISTS", List(key))(asBoolean)

  // DELETE (key1 key2 ..)
  // deletes the specified keys.
  def del(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    send("DEL", key :: keys.toList)(asLong)

  // TYPE (key)
  // return the type of the value stored at key in form of a string.
  def getType(key: Any)(implicit format: Format): Option[String] =
    send("TYPE", List(key))(asString)

  // EXPIRE (key, expiry)
  // sets the expire time (in sec.) for the specified key.
  def expire(key: Any, ttl: Int)(implicit format: Format): Boolean =
    send("EXPIRE", List(key, ttl))(asBoolean)

  // PEXPIRE (key, expiry)
  // sets the expire time (in milli sec.) for the specified key.
  def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): Boolean =
    send("PEXPIRE", List(key, ttlInMillis))(asBoolean)

  // EXPIREAT (key, unix timestamp)
  // sets the expire time for the specified key.
  def expireat(key: Any, timestamp: Long)(implicit format: Format): Boolean =
    send("EXPIREAT", List(key, timestamp))(asBoolean)

  // PEXPIREAT (key, unix timestamp)
  // sets the expire timestamp in millis for the specified key.
  def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): Boolean =
    send("PEXPIREAT", List(key, timestampInMillis))(asBoolean)

  // TTL (key)
  // returns the remaining time to live of a key that has a timeout
  def ttl(key: Any)(implicit format: Format): Option[Long] =
    send("TTL", List(key))(asLong)

  // PTTL (key)
  // returns the remaining time to live of a key that has a timeout in millis
  def pttl(key: Any)(implicit format: Format): Option[Long] =
    send("PTTL", List(key))(asLong)
    
  
  // FLUSHDB the DB
  // removes all the DB data.
  def flushdb: Boolean =
    send("FLUSHDB")(asBoolean)

  // FLUSHALL the DB's
  // removes data from all the DB's.
  def flushall: Boolean =
    send("FLUSHALL")(asBoolean)

  // MOVE
  // Move the specified key from the currently selected DB to the specified destination DB.
  def move(key: Any, db: Int)(implicit format: Format): Boolean =
    send("MOVE", List(key, db))(asBoolean)
  
  def scan[T](pattern: Option[Any] = None, limitOpt: Option[Int] = None, batchSize: Option[Int] = None)(f: List[String] => T) {
    var nextCursor = 0

    var params = List.empty[Any]
    pattern.foreach {
      p => params = params.::(p).::("match")
    }
    batchSize.foreach {
      b => params = params.::(b).::("count")
    }

    val process = () => {
      val rs = send("SCAN", params.::(nextCursor))(asScanResult[String])
      nextCursor = rs._1
      rs._2.getOrElse(List.empty).filter(_.isDefined).map(_.get)
    }: List[String]

    limitOpt match {
      case None =>
        do {
          f (process())
        } while (nextCursor != 0)
      case Some(limit) =>
        var totalLoadedKeys = 0
        do {
          var keys = process()
          totalLoadedKeys += keys.size
          val exceed = totalLoadedKeys - limit
          if (exceed > 0){
            keys = keys.slice(0, keys.size - exceed)
          }
          f (keys)
        }while (nextCursor != 0 && totalLoadedKeys < limit)
    }

  }
}
