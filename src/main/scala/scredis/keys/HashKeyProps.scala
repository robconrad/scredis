package scredis.keys

/**
  * Strongly typed set of hash field properties allows us to reason about hash key properties
  * @author rconrad
  */
case class HashKeyProps(props: Set[HashKeyProp]) {

  lazy val propMap = props.map(prop => prop.toString -> prop).toMap

  def apply(prop: String) = propMap.get(prop)

}
