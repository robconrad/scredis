package scredis.keys

import scala.concurrent.Future

/**
 * This trait specifies list key commands
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait ListKey[KP, K, V] extends Key[KP, K] {

  /**
   * Returns an element from a list by its index.
   *
   * @note The index is zero-based, so 0 means the first element, 1 the second element and so on.
   * Negative indices can be used to designate elements starting at the tail of the list.
   * Here, -1 means the last element, -2 means the penultimate and so forth.
   *
   * @param index zero-based position in the list
   * @return the requested element, or $none when index is out of range
   *
   * @since 1.0.0
   */
  def lIndex(index: Long): Future[Option[V]]

  /**
   * Inserts an element before or after another element in a list.
   *
   * @param pivot value after/before which the element should be inserted
   * @param value element to be inserted
   * @return the length of the list after the insert operation, or None if the index is out of range
   *
   * @since 2.2.0
   */
  def lInsert(position: scredis.Position, pivot: V, value: V): Future[Option[Long]]

  /**
   * Returns the length of a list.
   *
   * @return the length of the list at key, or 0 if the key does not exist
   *
   * @since 1.0.0
   */
  def lLen: Future[Long]

  /**
   * Removes and returns the first element of a list.
   *
   * @return the popped element, or $none if the key does not exist
   *
   * @since 1.0.0
   */
  def lPop(): Future[Option[V]]

  /**
   * Prepends one or multiple values to a list.
   *
   * @note If key does not exist, it is created as empty list before performing the push operation.
   * Redis versions older than 2.4 can only push one value per call.
   *
   * @param values value(s) to prepend
   * @return the length of the list after the push operations
   *
   * @since 1.0.0
   */
  def lPush(values: V*): Future[Long]

  /**
   * Prepends a value to a list, only if the list exists.
   *
   * @param value value to prepend
   * @return the length of the list after the push operation
   *
   * @since 2.2.0
   */
  def lPushX(value: V): Future[Long]

  /**
   * Returns a range of elements from a list.
   *
   * @note The offsets start and end are zero-based indexes, with 0 being the first element of the
   * list (the head of the list), 1 being the next element and so on. These offsets can also be
   * negative numbers indicating offsets starting at the end of the list. For example, -1 is the
   * last element of the list, -2 the penultimate, and so on. Both offsets are inclusive, i.e.
   * LRANGE key 0 10 will return 11 elements (if they exist).
   *
   * @param start start offset (inclusive)
   * @param stop stop offset (inclusive)
   * @return list of elements in the specified range, or the empty list if there are no such
   * elements or the key does not exist
   *
   * @since 1.0.0
   */
  def lRange(start: Long = 0, stop: Long = -1): Future[List[V]]

  /**
   * Removes the first count occurrences of elements equal to value from the list stored at key.
   *
   * @note The count argument influences the operation in the following ways:
   * {{{
   * count > 0: Remove elements equal to value moving from head to tail.
   * count < 0: Remove elements equal to value moving from tail to head.
   * count = 0: Remove all elements equal to value.
   * }}}
   *
   * @param value value to be removed from the list
   * @param count indicates the number of found values that should be removed, see above note
   * @return the number of removed elements
   *
   * @since 1.0.0
   */
  def lRem(value: V, count: Int = 0): Future[Long]

  /**
   * Sets the value of an element in a list by its index.
   *
   * @param index position of the element to set
   * @param value value to be set at index
   *
   * @since 1.0.0
   */
  def lSet(index: Long, value: V): Future[Unit]

  /**
   * Trims a list to the specified range.
   *
   * @note Out of range indexes will not produce an error: if start is larger than the end of the
   * list, or start > end, the result will be an empty list (which causes key to be removed). If
   * end is larger than the end of the list, Redis will treat it like the last element of the list.
   *
   * @param start start offset (inclusive)
   * @param stop stop offset (inclusive)
   *
   * @since 1.0.0
   */
  def lTrim(start: Long, stop: Long): Future[Unit]

  /**
   * Removes and returns the last element of a list.
   *
   * @return the popped element, or $none if the key does not exist
   *
   * @since 1.0.0
   */
  def rPop(): Future[Option[V]]

  /**
   * Removes the last element in a list, appends it to another list and returns it.
   *
   * @param destKey key of list to be push to
   * @return the popped element, or $none if the key does not exist
   *
   * @since 1.2.0
   */
  def rPopLPush(destKey: ListKey[_, _, V]): Future[Option[V]]

  /**
   * Appends one or multiple values to a list.
   *
   * @note If key does not exist, it is created as empty list before performing the push operation.
   * Redis versions older than 2.4 can only push one value per call.
   *
   * @param values value(s) to prepend
   * @return the length of the list after the push operations
   *
   * @since 1.0.0
   */
  def rPush(values: V*): Future[Long]

  /**
   * Appends a value to a list, only if the list exists.
   *
   * @param value value to prepend
   * @return the length of the list after the push operation
   *
   * @since 2.2.0
   */
  def rPushX(value: V): Future[Long]

}
