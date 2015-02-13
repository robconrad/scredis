package scredis.keys.impl

import scredis.Client
import scredis.commands.StringCommands
import scredis.keys.SimpleKey
import scredis.serialization.{Reader, Writer}

import scala.concurrent.duration.FiniteDuration

class SimpleKeyImpl[KP, K, V](val keyPrefix: KP,
                              val keyValue: K)
                              (implicit val client: Client,
                                        val keyPrefixWriter: Writer[KP],
                                        val keyWriter: Writer[K],
                                        val valueWriter: Writer[V],
                                        val valueReader: Reader[V])
    extends KeyValueImpl[KP, K, V]
    with SimpleKey[KP, K, V] {

  private lazy val commands: StringCommands = client

  def append(value: V) =
    commands.append(key, value)

  def bitCount(start: Long, stop: Long) =
    commands.bitCount(key, start, stop)

  def bitPos(bit: Boolean, start: Long, stop: Long) =
    commands.bitPos(key, bit, start, stop)

  def decr =
    commands.decr(key)

  def decrBy(decrement: Long) =
    commands.decrBy(key, decrement)

  def get =
    commands.get(key)

  def getBit(offset: Long) =
    commands.getBit(key, offset)

  def getRange(start: Long, stop: Long) =
    commands.getRange(key, start, stop)

  def getSet(value: V) =
    commands.getSet(key, value)

  def incr =
    commands.incr(key)

  def incrBy(increment: Long) =
    commands.incrBy(key, increment)

  def incrByFloat(increment: Double) =
    commands.incrByFloat(key, increment)

  def pSetEX(value: V, ttlMillis: Long) =
    commands.pSetEX(key, value, ttlMillis)

  def set(value: V, ttlOpt: Option[FiniteDuration], conditionOpt: Option[scredis.Condition]) =
    commands.set(key, value, ttlOpt, conditionOpt)

  def setBit(offset: Long, bit: Boolean) =
    commands.setBit(key, offset, bit)

  def setEX(value: V, ttlSeconds: Int) =
    commands.setEX(key, value, ttlSeconds)

  def setNX(value: V) =
    commands.setNX(key, value)

  def setRange(offset: Long, value: V) =
    commands.setRange(key, offset, value)

  def strLen =
    commands.strLen(key)

}
