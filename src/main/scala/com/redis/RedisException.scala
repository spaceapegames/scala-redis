package com.redis

abstract class RedisException(errorMessage: String) extends RuntimeException (errorMessage){
}

class RedisMasterNotFoundException(masterName: String) extends RedisException("mastername=%s".format(masterName)) {

}
