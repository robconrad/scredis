/*
 * Copyright (c) 2015 Robert Conrad - All Rights Reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * This file is proprietary and confidential.
 * Last modified by rconrad, 2/11/15 10:25 PM
 */

package scredis.keys.impl

import scredis.serialization.{Reader, Writer}

trait KeyValueImpl[KP, K, V] extends KeyImpl[KP, K] {

  implicit protected def valueWriter: Writer[V]
  implicit protected def valueReader: Reader[V]

}
