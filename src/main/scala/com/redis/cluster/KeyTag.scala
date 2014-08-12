package com.redis.cluster

trait KeyTag {
  def tag(key: Seq[Byte]): Option[Seq[Byte]]
}

object RegexKeyTag extends KeyTag {

  val tagStart = '{'.toByte
  val tagEnd = '}'.toByte

  def tag(key: Seq[Byte]) = {
    val start = key.indexOf(tagStart) + 1
    if (start > 0) {
      val end = key.indexOf(tagEnd, start)
      if (end > -1) Some(key.slice(start,end)) else None
    } else None
  }
}

object NoOpKeyTag extends KeyTag {
  def tag(key: Seq[Byte]) = Some(key)
}
