package com.redis

import org.slf4j.LoggerFactory

trait Log {
 private val log = LoggerFactory.getLogger(getClass)

 def ifTrace(message: => String) = if (log.isTraceEnabled) trace(message)
 def trace(message:String, values:Any*) =
     log trace checkFormat(message, values: _*)
 def trace(message:String, error:Throwable, values:Any*) =
     log trace (checkFormat(message, values: _*), error)

 def ifDebug(message: => String) = if (log.isDebugEnabled) debug(message)
 def debug(message:String, values:Any*) =
     log debug checkFormat(message, values: _*)
 def debug(message:String, error:Throwable, values:Any*) =
     log debug (checkFormat(message, values: _*), error)

 def ifInfo(message: => String) = if (log.isInfoEnabled) info(message)
 def info(message:String, values:Any*) =
     log info checkFormat(message, values: _*)
 def info(message:String, error:Throwable, values:Any*) =
     log info (checkFormat(message, values: _*), error)

 def ifWarn(message: => String) = if (log.isWarnEnabled) warn(message)
 def warn(message:String, values:Any*) =
     log warn checkFormat(message, values: _*)
 def warn(message:String, error:Throwable, values:Any*) =
     log warn (checkFormat(message, values: _*), error)

 def ifError(message: => String) = if (log.isErrorEnabled) error(message)
 def error(message:String, values:Any*) =
     log error checkFormat(message, values: _*)
 def error(message:String, error:Throwable, values:Any*) =
     log error (checkFormat(message, values: _*), error)

  def checkFormat(msg: String, refs: Any*): String = if (refs.size > 0) msg.format(refs: _*) else msg
}
