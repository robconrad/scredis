/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is proprietary and confidential.
 * Last modified by rconrad, 2/11/15 9:54 PM
 */

package scredis.keys.impl

import scredis.Client
import scredis.commands.KeyCommands
import scredis.keys.Key
import scredis.serialization.Writer

trait KeyImpl[KP, K] extends Key[KP, K] {

  protected def client: Client

  protected implicit lazy val dispatcher = client.dispatcher

  private lazy val commands: KeyCommands = client

  implicit def keyPrefixWriter: Writer[KP]
  implicit def keyWriter: Writer[K]

  final lazy val key = keyPrefixWriter.write(keyPrefix) ++ keyWriter.write(keyValue)

  def del() =
    commands.del(key).map(_ == 1)

  def dump =
    commands.dump(key)

  def exists =
    commands.exists(key)

  def expire(ttlSeconds: Int) =
    commands.expire(key, ttlSeconds)

  def expireAt(timestamp: Long) =
    commands.expireAt(key, timestamp)

  def move(database: Int) =
    commands.move(key, database)

  def objectRefCount =
    commands.objectRefCount(key)

  def objectEncoding =
    commands.objectEncoding(key)

  def objectIdleTime =
    commands.objectIdleTime(key)

  def persist() =
    commands.persist(key)

  def pExpire(ttlMillis: Long) =
    commands.pExpire(key, ttlMillis)

  def pExpireAt(timestampMillis: Long) =
    commands.pExpireAt(key, timestampMillis)

  def pTtl =
    commands.pTtl(key)

  def rename(newKey: Key[_, _]) =
    commands.rename(key, newKey.key)

  def renameNX(newKey: Key[_, _]) =
    commands.renameNX(key, newKey.key)

  def ttl =
    commands.ttl(key)

  def `type` =
    commands.`type`(key)

}
