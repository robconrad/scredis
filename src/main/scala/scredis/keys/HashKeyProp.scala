package scredis.keys

/**
 * Strongly typed hash field property identifier
 */
case class HashKeyProp(v: String) {
  override def toString = v
}
