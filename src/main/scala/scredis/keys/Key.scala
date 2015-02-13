package scredis.keys

import scala.concurrent.Future

/**
 * This trait specifies common key commands
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait Key[KP, K] {

  /**
   * Unique key-space identifier for this group of keys
   *
   * @return keyPrefix as a KP
   */
  def keyPrefix: KP

  /**
   * Returns the value that unique identifies this key within the keyPrefix key-space
   *
   * @return keyValue as a K
   */
  def keyValue: K

  /**
   * Returns the bare byte array that uniquely identifies this key
   *
   * @return key byte array
   */
  def key: Array[Byte]

  /**
   * Deletes one or multiple keys.
   *
   * @note a key is ignored if it does not exist
   *
   * @return the number of keys that were deleted
   *
   * @since 1.0.0
   */
  def del(): Future[Boolean]

  /**
   * Returns a serialized version of the value stored at the specified key.
   *
   * @return the serialized value or $none if the key does not exist
   *
   * @since 2.6.0
   */
  def dump: Future[Option[Array[Byte]]]

  /**
   * Determines if a key exists.
   *
   * @return $true if the key exists, $false otherwise
   *
   * @since 1.0.0
   */
  def exists: Future[Boolean]

  /**
   * Sets a key's time to live in seconds.
   *
   * @param ttlSeconds time-to-live in seconds
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 1.0.0
   */
  def expire(ttlSeconds: Int): Future[Boolean]

  /**
   * Sets the expiration for a key as a UNIX timestamp.
   *
   * @param timestamp  UNIX timestamp at which the key should expire
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 1.2.0
   */
  def expireAt(timestamp: Long): Future[Boolean]

  /**
   * Moves a key to another database.
   *
   * @param database destination database
   * @return true if key was moved, false otherwise
   *
   * @since 1.0.0
   */
  def move(database: Int): Future[Boolean]

  /**
   * Returns the number of references of the value associated with the specified key.
   *
   * @return the number of references or $none if the key does not exist
   *
   * @since 2.2.3
   */
  def objectRefCount: Future[Option[Long]]

  /**
   * Returns the kind of internal representation used in order to store the value associated with
   * a key.
   *
   * @note Objects can be encoded in different ways:
   * Strings can be encoded as `raw` or `int`
   * Lists can be encoded as `ziplist` or `linkedlist`
   * Sets can be encoded as `intset` or `hashtable`
   * Hashes can be encoded as `zipmap` or `hashtable`
   * SortedSets can be encoded as `ziplist` or `skiplist`
   *
   * @return the object encoding or $none if the key does not exist
   *
   * @since 2.2.3
   */
  def objectEncoding: Future[Option[String]]

  /**
   * Returns the number of seconds since the object stored at the specified key is idle (not
   * requested by read or write operations).
   *
   * @note While the value is returned in seconds the actual resolution of this timer is
   * 10 seconds, but may vary in future implementations.
   *
   * @return the number of seconds since the object is idle or $none if the key does not exist
   *
   * @since 2.2.3
   */
  def objectIdleTime: Future[Option[Long]]

  /**
   * Removes the expiration from a key.
   *
   * @return $true if key was persisted, $false if key does not exist or does not have an
   * associated timeout
   *
   * @since 2.2.0
   */
  def persist(): Future[Boolean]

  /**
   * Sets a key's time to live in milliseconds.
   *
   * @param ttlMillis time-to-live in milliseconds
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpire(ttlMillis: Long): Future[Boolean]

  /**
   * Sets the expiration for a key as a UNIX timestamp specified in milliseconds.
   *
   * @param timestampMillis  UNIX milliseconds-timestamp at which the key should expire
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpireAt(timestampMillis: Long): Future[Boolean]

  /**
   * Gets the time to live for a key in milliseconds.
   *
   * {{{
   * result match {
   *   case Left(false) => // key does not exist
   *   case Left(true) => // key exists but has no associated expire
   *   case Right(ttl) =>
   * }
   * }}}
   *
   * @note For `Redis` version <= 2.8.x, `Left(false)` will be returned when the key does not
   * exists and when it exists but has no associated expire (`Redis` returns the same error code
   * for both cases). In other words, you can simply check the following
   *
   * {{{
   * result match {
   *   case Left(_) =>
   *   case Right(ttl) =>
   * }
   * }}}
   *
   * @return `Right(ttl)` where ttl is the time-to-live in milliseconds for specified key,
   * `Left(false)` if key does not exist or `Left(true)` if key exists but has no associated
   * expire
   *
   * @since 2.6.0
   */
  def pTtl: Future[Either[Boolean, Long]]

  /**
   * Renames a key.
   *
   * @note if newKey already exists, it is overwritten
   * @param newKey destination key
   * does not exist
   *
   * @since 1.0.0
   */
  def rename(newKey: Key[_, _]): Future[Unit]

  /**
   * Renames a key, only if the new key does not exist.
   *
   * @param newKey destination key
   * @return $true if key was renamed to newKey, $false if newKey already exists
   *
   * @since 1.0.0
   */
  def renameNX(newKey: Key[_, _]): Future[Boolean]

  /**
   * Gets the time to live for a key in seconds.
   *
   * {{{
   * result match {
   *   case Left(false) => // key does not exist
   *   case Left(true) => // key exists but has no associated expire
   *   case Right(ttl) =>
   * }
   * }}}
   *
   * @note For `Redis` version <= 2.8.x, `Left(false)` will be returned when the key does not
   * exists and when it exists but has no associated expire (`Redis` returns the same error code
   * for both cases). In other words, you can simply check the following
   *
   * {{{
   * result match {
   *   case Left(_) =>
   *   case Right(ttl) =>
   * }
   * }}}
   *
   * @return `Right(ttl)` where ttl is the time-to-live in seconds for specified key,
   * `Left(false)` if key does not exist or `Left(true)` if key exists but has no associated
   * expire
   *
   * @since 1.0.0
   */
  def ttl: Future[Either[Boolean, Int]]

  /**
   * Determine the type stored at key.
   *
   * @note This method needs to be called as follows:
   * {{{
   * client.`type`(key)
   * }}}
   *
   * @return type of key, or $none if key does not exist
   *
   * @since 1.0.0
   */
  def `type`: Future[Option[scredis.Type]]

}
