/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is proprietary and confidential.
 * Last modified by rconrad, 2/11/15 9:51 PM
 */

package scredis.keys

import scredis.serialization.{Reader, Writer}

import scala.concurrent.Future

/**
 * This trait specifies hash key commands
 *  Special note that keys of this type are limited to fields defined as HashKeyProps
 *  which provides strong typing and allows us to better reason about hash key contents (not useful
 *  for hash keys with unknown properties - use bare commands for that)
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait HashKey[KP, K] extends Key[KP, K] {

  /**
   * Deletes one or more hash fields.
   *
   * @note Specified fields that do not exist within this hash are ignored. If key does not exist,
   * it is treated as an empty hash and this command returns 0. Redis versions older than 2.4 can
   * only remove a field per call.
   *
   * @param fields field(s) to be deleted from hash
   * @return the number of fields that were removed from the hash, not including specified but non
   * existing fields
   *
   * @since 2.0.0
   */
  def del(field: HashKeyProp, fields: HashKeyProp*): Future[Long]

  /**
   * Determines if a hash field exists.
   *
   * @param field name of the field
   * @return $true if the hash contains field, $false if the hash does not contain it or
   * the key does not exists
   *
   * @since 2.0.0
   */
  def exists(field: HashKeyProp): Future[Boolean]

  /**
   * Returns the value of a hash field.
   *
   * @param field field name to retrieve
   * @return the value associated with field name, or $none when field is not present in the hash
   * or key does not exist
   *
   * @since 2.0.0
   */
  def get[R: Reader](field: HashKeyProp): Future[Option[R]]

  /**
   * Returns all the fields and values in a hash.
   *
   * @return key-value pairs stored in hash with key, or $none when hash is empty or key does not
   * exist
   *
   * @since 2.0.0
   */
  def getAll[R: Reader]: Future[Option[Map[HashKeyProp, R]]]

  /**
   * Increments the integer value of a hash field by the given number.
   *
   * @note If key does not exist, a new key holding a hash is created. If field does not exist the
   * value is set to 0 before the operation is performed.
   *
   * @param field field name to increment
   * @param count increment
   * @return the value at field after the increment operation
   *
   * @since 2.0.0
   */
  def incrBy(field: HashKeyProp, count: Long): Future[Long]

  /**
   * Increments the float value of a hash field by the given amount.
   *
   * @note If key does not exist, a new key holding a hash is created. If field does not exist the
   * value is set to 0 before the operation is performed.
   *
   * @param field field name to increment
   * @param count increment
   * @return the value at field after the increment operation
   *
   * @since 2.6.0
   */
  def incrByFloat(field: HashKeyProp, count: Double): Future[Double]

  /**
   * Returns all the fields in a hash.
   *
   * @return set of field names or the empty set if the hash is empty or the key does not exist
   *
   * @since 2.0.0
   */
  def keys: Future[Set[HashKeyProp]]

  /**
   * Returns the number of fields contained in the hash stored at key.
   *
   * @return number of fields in the hash, or 0 if the key does not exist
   *
   * @since 2.0.0
   */
  def len: Future[Long]

  /**
   * Returns the values associated to the specified hash fields.
   *
   * @note For every field that does not exist, $none is returned.
   *
   * @param fields field(s) to retrieve
   * @return list of value(s) associated to the specified field name(s)
   *
   * @since 2.0.0
   */
  def mGet[R: Reader](fields: HashKeyProp*): Future[List[Option[R]]]

  /**
   * Returns a `Map` containing field-value pairs associated to the specified hash fields.
   *
   * @note Every non-existent field gets removed from the resulting `Map`.
   *
   * @param fields field(s) to retrieve
   * @return field-value pairs associated to the specified field name(s)
   *
   * @since 2.0.0
   */
  def mGetAsMap[R: Reader](fields: HashKeyProp*): Future[Map[HashKeyProp, R]]

  /**
   * Sets multiple hash fields to multiple values.
   *
   * @note This command overwrites any existing fields in the hash. If key does not exist, a new
   * key holding a hash is created
   *
   * @param fieldValuePairs field-value pair(s) to be set
   *
   * @since 2.0.0
   */
  def mSet[W: Writer](fieldValuePairs: Map[HashKeyProp, W]): Future[Unit]

  /**
   * Incrementally iterates through the fields of a hash.
   *
   * @param cursor the offset
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @return a pair containing the next cursor as its first element and the list of fields
   * (key-value pairs) as its second element
   *
   * @since 2.8.0
   */
  def scan[R: Reader](
    cursor: Long,
    matchOpt: Option[String] = None,
    countOpt: Option[Int] = None): Future[(Long, List[(HashKeyProp, R)])]

  /**
   * Sets the string value of a hash field.
   *
   * @note If the field already exists in the hash, it is overwritten.
   *
   * @param field field name to set
   * @param value value to set
   * @return $true if field is a new field in the hash and value was set, $false if
   * field already exists and the value was updated
   *
   * @since 2.0.0
   */
  def set[W: Writer](field: HashKeyProp, value: W): Future[Boolean]

  /**
   * Sets the value of a hash field, only if the field does not exist.
   *
   * @param field field name to set
   * @param value value to set
   * @return $true if field is a new field in the hash and value was set, $false if
   * field already exists and no operation was performed
   *
   * @since 2.0.0
   */
  def setNX[W: Writer](field: HashKeyProp, value: W): Future[Boolean]

  /**
   * Returns all the values in a hash.
   *
   * @return list of values, or the empty list if hash is empty or key does not exist
   *
   * @since 2.0.0
   */
  def vals[R: Reader]: Future[List[R]]

}
