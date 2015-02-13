package scredis.keys.impl

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions.RedisErrorResponseException
import scredis.keys.{HashKey, HashKeyProp, HashKeyProps}
import scredis.protocol.requests.HashRequests.{HDel, HSetNX, _}
import scredis.tags.{V200, V240, _}
import scredis.util.TestUtils._

class HashKeyImplSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {

  private implicit val client = Client()

  private val keyPrefix = "keyPrefix"

  private val FOO = HashKeyProp("FOO")
  private val BAR = HashKeyProp("BAR")
  private val FIELD = HashKeyProp("FIELD")
  private val FIELD1 = HashKeyProp("FIELD1")
  private val FIELD2 = HashKeyProp("FIELD2")
  private val FIELD3 = HashKeyProp("FIELD3")
  private val FIELD4 = HashKeyProp("FIELD4")
  private val FIELD5 = HashKeyProp("FIELD5")

  private implicit val props = HashKeyProps(Set(FOO, BAR, FIELD, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5))

  private def makeKey(myKey: String): HashKey[String, String] =
    new HashKeyImpl[String, String](keyPrefix, myKey)

  private val `HASH` = makeKey("HASH")
  private val `LIST` = makeKey("LIST")
  private val `NON-EXISTENT-KEY` = makeKey("NON-EXISTENT-KEY")
  
  private val SomeValue = "HelloWorld!虫àéç蟲"

  private val EXISTS = true
  private val NOT_EXISTS = false

  private val NEW_FIELD = true
  private val EXISTING_FIELD = false
  
  private val DELETED = true

  override def beforeAll() = {
    client.lPush(keyPrefix + "LIST", "A").futureValue
  }
  
  HDel.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V200 in {
        `NON-EXISTENT-KEY`.del(FOO).futureValue should be (0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.del(FOO).!
        }
      }
    }
    "the hash does not contain the field" should {
      "return 0" taggedAs V200 in {
        `HASH`.set(FIELD, SomeValue)
        `HASH`.del(FIELD2).futureValue should be (0)
      }
    }
    "the hash contains the field" should {
      "return 1" taggedAs V200 in {
        `HASH`.del(FIELD).futureValue should be (1)
        `HASH`.exists(FIELD).futureValue should be (NOT_EXISTS)
      }
    }
  }

  s"${HDel.toString} >= 2.4" when {
    "the key does not exist" should {
      "return 0" taggedAs V240 in {
        `NON-EXISTENT-KEY`.del(FOO, BAR).futureValue should be (0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V240 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.del(FOO, BAR).!
        }
      }
    }
    "the hash does not contain the fields" should {
      "return 0" taggedAs V240 in {
        `HASH`.set(FIELD, SomeValue)
        `HASH`.set(FIELD2, SomeValue)
        `HASH`.set(FIELD3, SomeValue)
        `HASH`.del(FIELD4, FIELD5).futureValue should be (0)
      }
    }
    "the hash contains the fields" should {
      "return the correct number of deleted fields" taggedAs V240 in {
        `HASH`.del(FIELD, FIELD4, FIELD5).futureValue should be (1)
        `HASH`.del(FIELD2, FIELD3).futureValue should be (2)
        `HASH`.exists(FIELD).futureValue should be (NOT_EXISTS)
        `HASH`.exists(FIELD2).futureValue should be (NOT_EXISTS)
        `HASH`.exists(FIELD3).futureValue should be (NOT_EXISTS)
      }
    }
  }

  HExists.toString when {
    "the key does not exist" should {
      "return false" taggedAs V200 in {
        `NON-EXISTENT-KEY`.exists(FOO).futureValue should be (NOT_EXISTS)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.exists(FOO).!
        }
      }
    }
    "the hash does not contain the field" should {
      "return false" taggedAs V200 in {
        `HASH`.set(FIELD, SomeValue)
        `HASH`.exists(FIELD2).futureValue should be (NOT_EXISTS)
      }
    }
    "the hash contains the field" should {
      "return true" taggedAs V200 in {
        `HASH`.exists(FIELD).futureValue should be (EXISTS)
        `HASH`.del(FIELD).futureValue should be (1)
      }
    }
  }

  HGet.toString when {
    "the key does not exist" should {
      "return None" taggedAs V200 in {
        `NON-EXISTENT-KEY`.get(FOO).futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.get(FOO).!
        }
      }
    }
    "the hash does not contain the field" should {
      "return None" taggedAs V200 in {
        `HASH`.set(FIELD, SomeValue)
        `HASH`.get(FIELD2).futureValue should be (empty)
      }
    }
    "the hash contains the field" should {
      "return the stored value" taggedAs V200 in {
        `HASH`.get(FIELD).futureValue should contain (SomeValue)
        `HASH`.del(FIELD).futureValue should be (1)
      }
    }
  }

  HGetAll.toString when {
    "the key does not exist" should {
      "return None" taggedAs V200 in {
        `NON-EXISTENT-KEY`.getAll.futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.getAll.!
        }
      }
    }
    "the hash contains some fields" should {
      "return the stored value" taggedAs V200 in {
        `HASH`.set(FIELD1, SomeValue)
        `HASH`.set(FIELD2, "YES")
        `HASH`.getAll.futureValue should contain (
          Map(FIELD1 -> SomeValue, FIELD2 -> "YES")
        )
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HIncrBy.toString when {
    "the key does not exist" should {
      "create a hash with the specified field and increment the field" taggedAs V200 in {
        `HASH`.incrBy(FIELD, 1).futureValue should be (1)
        `HASH`.get(FIELD).futureValue should contain ("1")
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.incrBy(FOO, 1).!
        }
      }
    }
    "the hash does not contain the field" should {
      "create the field and increment it" taggedAs V200 in {
        `HASH`.incrBy(FIELD2, 3).futureValue should be (3)
        `HASH`.get(FIELD2).futureValue should contain ("3")
      }
    }
    "the hash contains the field but the latter does not contain an integer" should {
      "return an error" taggedAs V200 in {
        `HASH`.set(FIELD3, SomeValue)
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.incrBy(FIELD3, 2).!
        }
      }
    }
    "the hash contains the field and the latter is an integer" should {
      "increment the value" taggedAs V200 in {
        `HASH`.incrBy(FIELD, -3).futureValue should be (-2)
        `HASH`.get(FIELD).futureValue should contain ("-2")
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HIncrByFloat.toString when {
    "the key does not exist" should {
      "create a hash with the specified field and increment the field" taggedAs V260 in {
        `HASH`.incrByFloat(FIELD, 1.2).futureValue should be (1.2)
        `HASH`.get(FIELD).futureValue should contain ("1.2")
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V260 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.incrByFloat(FOO, 1.2).!
        }
      }
    }
    "the hash does not contain the field" should {
      "create the field and increment it" taggedAs V260 in {
        `HASH`.incrByFloat(FIELD2, 3.4).futureValue should be (3.4)
        `HASH`.get(FIELD2).futureValue should contain ("3.4")
      }
    }
    "the hash contains the field but the latter does not contain a floating point number" should {
      "return an error" taggedAs V260 in {
        `HASH`.set(FIELD3, SomeValue)
        a [RedisErrorResponseException] should be thrownBy { 
          `HASH`.incrByFloat(FIELD3, 2.1).!
        }
      }
    }
    "the hash contains the field and the latter is a floating point number" should {
      "increment the value" taggedAs V260 in {
        `HASH`.incrByFloat(FIELD, -3.1).futureValue should be (-1.9)
        `HASH`.get(FIELD).futureValue should contain ("-1.9")
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HKeys.toString when {
    "the key does not exist" should {
      "return None" taggedAs V200 in {
        `NON-EXISTENT-KEY`.keys.futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy { `LIST`.keys.! }
      }
    }
    "the hash contains some fields" should {
      "return field names" taggedAs V200 in {
        `HASH`.set(FIELD1, SomeValue)
        `HASH`.set(FIELD2, "YES")
        `HASH`.keys.futureValue should contain theSameElementsAs List(FIELD1, FIELD2)
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V200 in {
        `NON-EXISTENT-KEY`.len.futureValue should be (0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy { `LIST`.len.! }
      }
    }
    "the hash contains some fields" should {
      "return the number of fields in the hash" taggedAs V200 in {
        `HASH`.set(FIELD1, SomeValue)
        `HASH`.set(FIELD2, "YES")
        `HASH`.len.futureValue should be (2)
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HMGet.toString when {
    "the key does not exist" should {
      "return a list contianing only None" taggedAs V200 in {
        `NON-EXISTENT-KEY`.mGet(FOO, BAR).futureValue should contain theSameElementsAs List(None, None)
        `NON-EXISTENT-KEY`.mGetAsMap(FOO, BAR).futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.mGet(FOO, BAR).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.mGetAsMap(FOO, BAR).!
        }
      }
    }
    "the hash contains some fields" should {
      "return the values" taggedAs V200 in {
        `HASH`.set(FIELD, SomeValue)
        `HASH`.set(FIELD2, "YES")
        `HASH`.mGet(
          FIELD, FIELD2, FIELD3
        ).futureValue should contain theSameElementsAs List(Some(SomeValue), Some("YES"), None)
        `HASH`.mGetAsMap(FIELD, FIELD2, FIELD3).futureValue should be (
          Map(FIELD -> SomeValue, FIELD2 -> "YES")
        )
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HMSet.toString when {
    "the key does not exist" should {
      "create a hash and set all specified fields" taggedAs V200 in {
        `HASH`.mSet(Map(FIELD -> SomeValue, FIELD2 -> "YES")).futureValue should be (())
        `HASH`.mGet(FIELD, FIELD2).futureValue should contain theSameElementsAs List(Some(SomeValue), Some("YES"))
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy { 
          `LIST`.mSet(Map(FIELD -> SomeValue, FIELD2 -> "YES")).!
        }
      }
    }
    "the hash contains some fields" should {
      "set and overwrite fields" taggedAs V200 in {
        `HASH`.mSet(
          Map(FIELD -> "NO", FIELD2 -> "", FIELD3 -> "YES")
        ).futureValue should be (())
        `HASH`.mGet(FIELD, FIELD2, FIELD3).futureValue should be (
          List(Some("NO"), Some(""), Some("YES"))
        )
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }
  
  HScan.toString when {
    "the key does not exist" should {
      "return an empty set" taggedAs V280 in {
        val (next, list) = `NON-EXISTENT-KEY`.scan(0).!
        next should be (0)
        list should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V280 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.scan(0).!
        }
      }
    }
  }

  HSet.toString when {
    "the key does not exist" should {
      "create the hash and set the given field" taggedAs V200 in {
        `HASH`.set(FIELD, SomeValue).futureValue should be (NEW_FIELD)
        `HASH`.get(FIELD).futureValue should contain (SomeValue)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.set(FOO, "bar").!
        }
      }
    }
    "the hash already exists and does not contain the field" should {
      "set the new field" taggedAs V200 in {
        `HASH`.set(FIELD2, "YES").futureValue should be (NEW_FIELD)
        `HASH`.get(FIELD2).futureValue should contain ("YES")
      }
    }
    "the hash already contains the field" should {
      "overwrite the value" taggedAs V200 in {
        `HASH`.set(FIELD, "NEW").futureValue should be (EXISTING_FIELD)
        `HASH`.get(FIELD).futureValue should contain ("NEW")
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HSetNX.toString when {
    "the key does not exist" should {
      "create the hash and set the given field" taggedAs V200 in {
        `HASH`.setNX(FIELD, SomeValue).futureValue should be (NEW_FIELD)
        `HASH`.get(FIELD).futureValue should contain (SomeValue)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.setNX(FOO, "bar").!
        }
      }
    }
    "the hash already exists and does not contain the field" should {
      "set the new field" taggedAs V200 in {
        `HASH`.setNX(FIELD2, "YES").futureValue should be (NEW_FIELD)
        `HASH`.get(FIELD2).futureValue should contain ("YES")
      }
    }
    "the hash already contains the field" should {
      "return an error" taggedAs V200 in {
        `HASH`.setNX(FIELD, "NEW").futureValue should be (EXISTING_FIELD)
        `HASH`.get(FIELD).futureValue should contain (SomeValue)
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  HVals.toString when {
    "the key does not exist" should {
      "return None" taggedAs V200 in {
        `NON-EXISTENT-KEY`.vals.futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.vals.!
        }
      }
    }
    "the hash contains some fields" should {
      "return field names" taggedAs V200 in {
        `HASH`.set(FIELD1, SomeValue)
        `HASH`.set(FIELD2, "YES")
        `HASH`.set(FIELD3, "YES")
        `HASH`.vals.futureValue should contain theSameElementsAs List(SomeValue, "YES", "YES")
        `HASH`.del().futureValue should be (DELETED)
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }

}
