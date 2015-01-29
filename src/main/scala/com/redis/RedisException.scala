package com.redis

abstract class RedisException(errorMessage: String) extends RuntimeException (errorMessage){
}

case class RedisConnectionException(message: String) extends RedisException(message)
case class RedisMultiExecException(message: String) extends RedisException(message)

class RedisMasterNotFoundException(masterName: String) extends RedisException("mastername=%s".format(masterName)) {

}

class RedisProtocolParsingException(msg: String) extends RedisException(msg){

}
