package scredis.keys

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * This trait specifies simple key commands (elsewhere known as StringCommands but called
 *  simple here because they can be of types other than String)
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait SimpleKey[KP, K, V] extends Key[KP, K] {

  /**
   * Appends a value to a key.
   *
   * @param value the value to append
   * @return the length of the string after the append operation
   *
   * @since 2.0.0
   */
  def append(value: V): Future[Long]

  /**
   * Counts the number of bits set to 1 in a string from start offset to stop offset.
   *
   * @note Non-existent keys are treated as empty strings, so the command will return zero.
   *
   * @param start start offset (defaults to 0)
   * @param stop stop offset (defaults to -1)
   * @return the number of bits set to 1 in the specified interval
   *
   * @since 2.6.0
   */
  def bitCount(start: Long = 0, stop: Long = -1): Future[Long]

  /**
   * Return the position of the first bit set to 1 or 0 in a string.
   *
   * @note The position is returned thinking at the string as an array of bits from left to right
   * where the first byte most significant bit is at position 0, the second byte most significant
   * big is at position 8 and so forth.
   *
   * The range is interpreted as a range of bytes and not a range of bits, so start=0 and end=2
   * means to look at the first three bytes.
   *
   * If we look for set bits (the bit argument is 1) and the string is empty or composed of just
   * zero bytes, -1 is returned.
   *
   * If we look for clear bits (the bit argument is 0) and the string only contains bit set to 1,
   * the function returns the first bit not part of the string on the right. So if the string is
   * tree bytes set to the value 0xff the command BITPOS key 0 will return 24, since up to bit 23
   * all the bits are 1.
   *
   * Basically the function consider the right of the string as padded with zeros if you look for
   * clear bits and specify no range or the start argument '''only'''.
   *
   * However this behavior changes if you are looking for clear bits and specify a range with both
   * start and stop. If no clear bit is found in the specified range, the function returns -1 as
   * the user specified a clear range and there are no 0 bits in that range.
   *
   * @param bit provide $true to look for 1s and $false to look for 0s
   * @param start start offset, in bytes
   * @param stop stop offset, in bytes
   * @return the position of the first bit set to 1 or 0, according to the request
   *
   * @since 2.8.7
   */
  def bitPos(bit: Boolean, start: Long = 0, stop: Long = -1): Future[Long]

  /**
   * Decrements the integer value of a key by one.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @return the value of key after the decrement
   * represented as integer
   *
   * @since 1.0.0
   */
  def decr: Future[Long]

  /**
   * Decrements the integer value of a key by the given amount.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param decrement the decrement
   * @return the value of key after the decrement
   * a string that cannot be represented as integer
   *
   * @since 1.0.0
   */
  def decrBy(decrement: Long): Future[Long]

  /**
   * Returns the value stored at key.
   *
   * @return value stored at key, or $none if the key does not exist
   *
   * @since 1.0.0
   */
  def get: Future[Option[V]]

  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param offset the position in the string
   * @return $true if the bit is set to 1, $false otherwise
   *
   * @since 2.2.0
   */
  def getBit(offset: Long): Future[Boolean]

  /**
   * Returns a substring of the string stored at a key.
   *
   * @note Both offsets are inclusive, i.e. [start, stop]. The function handles out of range
   * requests by limiting the resulting range to the actual length of the string.
   *
   * @param start the start offset (inclusive)
   * @param stop the stop offset (inclusive)
   * @return the substring determined by the specified offsets, or the empty string if the key
   * does not exist
   *
   * @since 2.4.0
   */
  def getRange(start: Long, stop: Long): Future[V]

  /**
   * Sets the string value of a key and return its old value.
   *
   * @param value the value to set key to
   * @return the old value, or $none if the latter did not exist
   *
   * @since 1.0.0
   */
  def getSet(value: V): Future[Option[V]]

  /**
   * Increments the integer value of a key by one.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @return the value of key after the increment
   * represented as integer
   *
   * @since 1.0.0
   */
  def incr: Future[Long]

  /**
   * Increments the integer value of a key by the given amount.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param increment the increment
   * @return the value of key after the decrement
   * a string that cannot be represented as integer
   *
   * @since 1.0.0
   */
  def incrBy(increment: Long): Future[Long]

  /**
   * Increment the float value of a key by the given amount.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param increment the increment
   * @return the value of key after the decrement
   * specified increment are not parseable as a double precision floating point number
   *
   * @since 2.6.0
   */
  def incrByFloat(increment: Double): Future[Double]

  /**
   * Sets the value and expiration in milliseconds of a key.
   *
   * @note If key already holds a value, it is overwritten, regardless of its type.
   *
   * @param value value to be stored at key
   * @param ttlMillis time-to-live in milliseconds
   *
   * @since 2.6.0
   */
  def pSetEX(value: V, ttlMillis: Long): Future[Unit]

  /**
   * Sets the string value of a key.
   *
   * @note If key already holds a value, it is overwritten, regardless of its type. Any previous
   * time to live associated with the key is discarded on successful SET operation.
   *
   * The ttlOpt and conditionOpt parameters can only be used with `Redis` >= 2.6.12
   *
   * @param value value to be stored at key
   * @param ttlOpt optional time-to-live (up to milliseconds precision)
   * @param conditionOpt optional condition to be met for the value to be set
   * @return $true if the value was set correctly, $false if a condition was specified but not met
   *
   * @since 1.0.0
   */
  def set(
    value: V,
    ttlOpt: Option[FiniteDuration] = None,
    conditionOpt: Option[scredis.Condition] = None): Future[Boolean]

  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @note When key does not exist, a new string value is created. The string is grown to make sure
   * it can hold a bit at offset. When the string at key is grown, added bits are set to 0.
   *
   * @param offset position where the bit should be set
   * @param bit $true sets the bit to 1, $false sets it to 0
   *
   * @since 2.2.0
   */
  def setBit(offset: Long, bit: Boolean): Future[Boolean]

  /**
   * Sets the value and expiration in seconds of a key.
   *
   * @note If key already holds a value, it is overwritten, regardless of its type.
   *
   * @param value value to be stored at key
   * @param ttlSeconds time-to-live in seconds
   *
   * @since 2.0.0
   */
  def setEX(value: V, ttlSeconds: Int): Future[Unit]

  /**
   * Sets the value of a key, only if the key does not exist.
   *
   * @param value value to be stored at key
   * @return $true if the key was set, $false otherwise
   *
   * @since 1.0.0
   */
  def setNX(value: V): Future[Boolean]

  /**
   * Overwrites part of a string at key starting at the specified offset.
   *
   * @note If the offset is larger than the current length of the string at key, the string is
   * padded with zero-bytes to make offset fit. Non-existing keys are considered as empty strings,
   * so this command will make sure it holds a string large enough to be able to set value at
   * offset.
   *
   * @param offset position from which the string must be overwritten
   * @param value string value to be set at given offset
   * @return the length of the string after it was modified by the command
   *
   * @since 2.2.0
   */
  def setRange(offset: Long, value: V): Future[Long]

  /**
   * Returns the length of the string value stored in a key.
   *
   * @return the length of the string stored at key, or 0 when the key does not exist
   *
   * @since 2.2.0
   */
  def strLen: Future[Long]

}
