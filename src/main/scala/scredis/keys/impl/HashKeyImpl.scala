/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is HashKeyProprietary and confidential.
 * Last modified by rconrad, 2/11/15 10:25 PM
 */

package scredis.keys.impl

import scredis.Client
import scredis.commands.HashCommands
import scredis.keys.{HashKey, HashKeyProp, HashKeyProps}
import scredis.serialization.{Reader, Writer}

class HashKeyImpl[KP, K](val keyPrefix: KP,
                         val keyValue: K)
                        (implicit val client: Client,
                                  val props: HashKeyProps,
                                  val keyPrefixWriter: Writer[KP],
                                  val keyWriter: Writer[K])
    extends KeyImpl[KP, K]
    with HashKey[KP, K] {

  protected implicit def prop2String(p: HashKeyProp): String = p.toString
  protected implicit def props2String(p: Seq[HashKeyProp]): Seq[String] = p.map(_.toString)

  private lazy val commands: HashCommands = client

  def del(field: HashKeyProp, fields: HashKeyProp*) =
    commands.hDel(key, field +: fields: _*)

  def exists(field: HashKeyProp) =
    commands.hExists(key, field)

  def get[R: Reader](field: HashKeyProp) =
    commands.hGet(key, field)

  def getAll[R: Reader] =
    commands.hGetAll(key).map(_.map(_.map {
      case (prop, value) => props(prop).orNull -> value
    }.toMap))

  def incrBy(field: HashKeyProp, count: Long) =
    commands.hIncrBy(key, field, count)

  def incrByFloat(field: HashKeyProp, count: Double) =
    commands.hIncrByFloat(key, field, count)

  def keys =
    commands.hKeys(key).map(_.map(props(_).orNull))

  def len =
    commands.hLen(key)

  def mGet[R: Reader](fields: HashKeyProp*) =
    commands.hmGet(key, fields: _*)

  def mGetAsMap[R: Reader](fields: HashKeyProp*) =
    commands.hmGetAsMap(key, fields: _*).map(_.map {
      case (k, v) => props(k).orNull -> v
    })

  def mSet[W: Writer](fieldValuePairs: Map[HashKeyProp, W]) =
    commands.hmSet(key, fieldValuePairs.map {
      case (k, v) =>
        k.toString -> v
    })

  def scan[R: Reader](cursor: Long, matchOpt: Option[String], countOpt: Option[Int]) =
    commands.hScan(key, cursor, matchOpt, countOpt).map {
      case (cursor, fields) => cursor -> fields.map {
        case (field, reader) => (props(field).orNull, reader)
      }
    }

  def set[W: Writer](field: HashKeyProp, value: W) =
    commands.hSet(key, field, value)

  def setNX[W: Writer](field: HashKeyProp, value: W) =
    commands.hSetNX(key, field, value)

  def vals[R: Reader] =
    commands.hVals(key)

}
