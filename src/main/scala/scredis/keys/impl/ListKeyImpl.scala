/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is proprietary and confidential.
 * Last modified by rconrad, 2/11/15 10:25 PM
 */

package scredis.keys.impl

import scredis.Client
import scredis.commands.ListCommands
import scredis.keys.ListKey
import scredis.serialization.{Reader, Writer}

class ListKeyImpl[KP, K, V](val keyPrefix: KP,
                            val keyValue: K)
                           (implicit val client: Client,
                                     val keyPrefixWriter: Writer[KP],
                                     val keyWriter: Writer[K],
                                     val valueWriter: Writer[V],
                                     val valueReader: Reader[V])
    extends KeyValueImpl[KP, K, V]
    with ListKey[KP, K, V] {

  private lazy val commands: ListCommands = client

  def lIndex(index: Long) =
    commands.lIndex(key, index)

  def lInsert(position: scredis.Position, pivot: V, value: V) =
    commands.lInsert(key, position, pivot, value)

  def lLen =
    commands.lLen(key)

  def lPop() =
    commands.lPop(key)

  def lPush(values: V*) =
    commands.lPush(key, values: _*)

  def lPushX(value: V) =
    commands.lPushX(key, value)

  def lRange(start: Long, stop: Long) =
    commands.lRange(key, start, stop)

  def lRem(value: V, count: Int) =
    commands.lRem(key, value, count)

  def lSet(index: Long, value: V) =
    commands.lSet(key, index, value)

  def lTrim(start: Long, stop: Long) =
    commands.lTrim(key, start, stop)

  def rPop() =
    commands.rPop(key)

  def rPopLPush(destKey: ListKey[_, _, V]) =
    commands.rPopLPush(key, destKey.key)

  def rPush(values: V*) =
    commands.rPush(key, values: _*)

  def rPushX(value: V) =
    commands.rPushX(key, value)

}
