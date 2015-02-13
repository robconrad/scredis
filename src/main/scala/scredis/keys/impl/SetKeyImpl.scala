/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is proprietary and confidential.
 * Last modified by rconrad, 2/11/15 10:25 PM
 */

package scredis.keys.impl

import scredis.Client
import scredis.commands.SetCommands
import scredis.keys.SetKey
import scredis.serialization.{Reader, Writer}

class SetKeyImpl[KP, K, V](val keyPrefix: KP,
                           val keyValue: K)
                          (implicit val client: Client,
                                    val keyPrefixWriter: Writer[KP],
                                    val keyWriter: Writer[K],
                                    val valueWriter: Writer[V],
                                    val valueReader: Reader[V])
    extends KeyValueImpl[KP, K, V]
    with SetKey[KP, K, V] {

  private lazy val commands: SetCommands = client

  def add(members: V*) =
    commands.sAdd(key, members: _*)

  def card =
    commands.sCard(key)

  def diff(keys: SetKey[_, _, V]*) =
    commands.sDiff(key, keys.map(_.key): _*)

  def diffStore(key1: SetKey[_, _, V], keys: SetKey[_, _, V]*) =
    commands.sDiffStore(key, key1.key, keys.map(_.key): _*)

  def inter(keys: SetKey[_, _, V]*) =
    commands.sInter(key +: keys.map(_.key): _*)

  def interStore(keys: SetKey[_, _, V]*) =
    commands.sInterStore(key, keys.map(_.key): _*)

  def isMember(member: V) =
    commands.sIsMember(key, member)

  def members =
    commands.sMembers(key)

  def move(destKey: SetKey[_, _, V], member: V) =
    commands.sMove(key, destKey.key, member)

  def pop() =
    commands.sPop(key)

  def randMember =
    commands.sRandMember(key)

  def randMembers(count: Int) =
    commands.sRandMembers(key, count)

  def rem(members: V*) =
    commands.sRem(key, members: _*)

  def scan(cursor: Long, matchOpt: Option[String], countOpt: Option[Int]) =
    commands.sScan(key, cursor, matchOpt, countOpt)

  def union(keys: SetKey[_, _, V]*) =
    commands.sUnion(key +: keys.map(_.key): _*)

  def unionStore(keys: SetKey[_, _, V]*) =
    commands.sUnionStore(key, keys.map(_.key): _*)

}
