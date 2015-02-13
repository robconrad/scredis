/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is proprietary and confidential.
 * Last modified by rconrad, 2/11/15 9:51 PM
 */

package scredis.keys

import scala.concurrent.Future

/**
 * This trait specifies set key commands
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait SetKey[KP, K, V] extends Key[KP, K] {

  /**
   * Adds one or more members to a set.
   *
   * @param members member(s) to add
   * @return the number of members added to the set, not including all the members that were
   * already present
   *
   * @since 1.0.0
   */
  def add(members: V*): Future[Long]

  /**
   * Returns the number of members in a set.
   *
   * @return the cardinality (number of members) of the set, or 0 if key does not exist
   *
   * @since 1.0.0
   */
  def card: Future[Long]

  /**
   * Returns the set resulting from the difference between the first set and all the successive
   * sets.
   *
   * @param keys key(s) of successive set(s) whose members will be subtracted from the first one
   * @return the resulting set, or the empty set if the first key does not exist
   *
   * @since 1.0.0
   */
  def diff(keys: SetKey[_, _, V]*): Future[Set[V]]

  /**
   * Stores the set resulting from the difference between the first set and all the successive sets.
   *
   * @note If this key already exists, it is overwritten.
   *
   * @param key1 key of first set
   * @param keys keys of sets to be subtracted from first set, if empty, first set is simply
   * copied to destKey
   * @return the cardinality of the resulting set
   *
   * @since 1.0.0
   */
  def diffStore(key1: SetKey[_, _, V], keys: SetKey[_, _, V]*): Future[Long]

  /**
   * Intersects multiple sets.
   *
   * @param keys keys of sets to be intersected with this one
   * @return the resulting set, or the empty set if the first key does not exist
   *
   * @since 1.0.0
   */
  def inter(keys: SetKey[_, _, V]*): Future[Set[V]]

  /**
   * Intersects multiple sets and stores the resulting set in a key.
   *
   * @note If this key already exists, it is overwritten.
   *
   * @param keys keys of sets to be intersected together, if only one is specified, it is simply
   * copied to this key
   * @return the cardinality of the resulting set
   *
   * @since 1.0.0
   */
  def interStore(keys: SetKey[_, _, V]*): Future[Long]

  /**
   * Determines if a given value is a member of a set.
   *
   * @param member value to be tested
   * @return $true if the provided value is a member of the set stored at key, $false otherwise
   *
   * @since 1.0.0
   */
  def isMember(member: V): Future[Boolean]

  /**
   * Returns all the members of a set.
   *
   * @return set stored at key, or the empty set if key does not exist
   *
   * @since 1.0.0
   */
  def members: Future[Set[V]]

  /**
   * Moves a member from one set to another.
   *
   * @param destKey key of destination set
   * @param member value to be moved from source set to destination set
   * @return $true if the member was moved, $false if the element is not a member of source set and
   * no operation was performed
   *
   * @since 1.0.0
   */
  def move(destKey: SetKey[_, _, V], member: V): Future[Boolean]

  /**
   * Removes and returns a random member from a set.
   *
   * @note This operation is similar to SRANDMEMBER, that returns a random element from a set but
   * does not remove it.
   *
   * @return random member, or $none if key does not exist
   *
   * @since 1.0.0
   */
  def pop(): Future[Option[V]]

  /**
   * Returns a random member from a set (without removing it).
   *
   * @return random member, or $none if key does not exist
   *
   * @since 1.0.0
   */
  def randMember: Future[Option[V]]

  /**
   * Returns a random member from a set (without removing it).
   *
   * @param count number of member to randomly retrieve
   * @return set of random members, or the empty set if key does not exist
   *
   * @since 2.6.0
   */
  def randMembers(count: Int = 1): Future[Set[V]]

  /**
   * Removes one or more members from a set.
   *
   * @note Redis versions older than 2.4 can only remove one member per call.
   *
   * @param members members to remove from set
   * @return the number of members that were removed from the set, not including non-existing
   * members
   *
   * @since 1.0.0
   */
  def rem(members: V*): Future[Long]

  /**
   * Incrementally iterates the elements of a set.
   *
   * @param cursor the offset
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @return a pair containing the next cursor as its first element and the set of elements
   * as its second element
   *
   * @since 2.8.0
   */
  def scan(cursor: Long, matchOpt: Option[String] = None, countOpt: Option[Int] = None): Future[(Long, Set[V])]

  /**
   * Computes the union of multiple sets.
   *
   * @param keys keys of sets to be included in the union computation
   * @return the resulting set, or the empty set if the first key does not exist
   *
   * @since 1.0.0
   */
  def union(keys: SetKey[_, _, V]*): Future[Set[V]]

  /**
   * Computes the union of multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param keys keys of sets to be included in the union computation, if only one is specified,
   * it is simply copied to destKey
   * @return the cardinality of the resulting set
   *
   * @since 1.0.0
   */
  def unionStore(keys: SetKey[_, _, V]*): Future[Long]

}
