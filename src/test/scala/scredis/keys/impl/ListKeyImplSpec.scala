package scredis.keys.impl

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions.RedisErrorResponseException
import scredis.keys.ListKey
import scredis.protocol.requests.ListRequests.{RPush, RPushX, _}
import scredis.tags.{V240, _}
import scredis.util.TestUtils._

class ListKeyImplSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {

  private implicit val client = Client()

  private val keyPrefix = "keyPrefix"

  private def makeKey(myKey: String): ListKey[String, String, String] =
    new ListKeyImpl[String, String, String](keyPrefix, myKey)
  
  private val `LIST` = makeKey("LIST")
  private val `LIST2` = makeKey("LIST2")
  private val `LIST3` = makeKey("LIST3")
  private val `HASH` = makeKey("HASH")
  
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll(): Unit = {
    client.hSet(keyPrefix + "HASH", "FIELD", SomeValue).!
  }

  LIndex.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        `LIST`.lIndex(0).futureValue should be (empty)
        `LIST`.lIndex(100).futureValue should be (empty)
        `LIST`.lIndex(-1).futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { `HASH`.lIndex(0).! }
      }
    }
    "the index is out of range" should {
      "return None" taggedAs V100 in {
        `LIST`.rPush("Hello")
        `LIST`.rPush("World")
        `LIST`.rPush("!")
        `LIST`.lIndex(-100).futureValue should be (empty)
        `LIST`.lIndex(100).futureValue should be (empty)
      }
    }
    "the index is correct" should {
      "return the value stored at index" taggedAs V100 in {
        `LIST`.lIndex(0).futureValue should contain ("Hello")
        `LIST`.lIndex(1).futureValue should contain ("World")
        `LIST`.lIndex(2).futureValue should contain ("!")

        `LIST`.lIndex(-1).futureValue should contain ("!")
        `LIST`.lIndex(-2).futureValue should contain ("World")
        `LIST`.lIndex(-3).futureValue should contain ("Hello")
        `LIST`.del()
      }
    }
  }

  LInsert.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs V220 in {
        `LIST`.lInsert(Position.Before, "A", SomeValue).futureValue should contain (0)
        `LIST`.lPop().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lInsert(Position.Before, "A", SomeValue).!
        }
      }
    }
    "the pivot is not in the list" should {
      "return None" taggedAs V220 in {
        `LIST`.rPush("A")
        `LIST`.rPush("C")
        `LIST`.rPush("E")
        `LIST`.lInsert(Position.Before, "X", SomeValue).futureValue should be (empty)
      }
    }
    "the pivot is in the list" should {
      "insert the element at the correct position" taggedAs V220 in {
        `LIST`.lInsert(Position.After, "A", "B").futureValue should contain (4)
        `LIST`.lInsert(Position.Before, "E", "D").futureValue should contain (5)
        `LIST`.lInsert(Position.After, "E", "G").futureValue should contain (6)
        `LIST`.lInsert(Position.Before, "G", "F").futureValue should contain (7)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "E", "F", "G"
        )
        `LIST`.del()
      }
    }
  }

  LLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V100 in {
        `LIST`.lLen.futureValue should be (0)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { `HASH`.lLen.! }
      }
    }
    "the list contains some elements" should {
      "return the number of elements contained in the list" taggedAs V100 in {
        `LIST`.rPush("A")
        `LIST`.rPush("B")
        `LIST`.lLen.futureValue should be (2)
        `LIST`.del()
      }
    }
  }

  LPop.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        `LIST`.lPop().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { `HASH`.lPop().! }
      }
    }
    "the list contains some elements" should {
      "pop the first element of the list" taggedAs V100 in {
        `LIST`.rPush("A")
        `LIST`.rPush("B")
        `LIST`.lPop().futureValue should contain ("A")
        `LIST`.lPop().futureValue should contain ("B")
        `LIST`.lPop().futureValue should be (empty)
      }
    }
  }

  LPush.toString when {
    "the key does not exist" should {
      "create a list and prepend the value" taggedAs V100 in {
        `LIST`.lPush("A").futureValue should be (1)
        `LIST`.lPush("B").futureValue should be (2)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("B", "A")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lPush("A").!
        }
      }
    }
    "the list contains some elements" should {
      "prepend the value to the existing list" taggedAs V100 in {
        `LIST`.lPush("1").futureValue should be (3)
        `LIST`.lPush("2").futureValue should be (4)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "2", "1", "B", "A"
        )
        `LIST`.del()
      }
    }
  }

  s"${LPush.toString}-2.4" when {
    "the key does not exist" should {
      "create a list and prepend the values" taggedAs V240 in {
        `LIST`.lPush("A", "B").futureValue should be (2)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("B", "A")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V240 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lPush("A", "B").!
        }
      }
    }
    "the list contains some elements" should {
      "prepend the values to the existing list" taggedAs V240 in {
        `LIST`.lPush("1", "2").futureValue should be (4)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "2", "1", "B", "A"
        )
        `LIST`.del()
      }
    }
  }

  LPushX.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs V220 in {
        `LIST`.lPushX("A").futureValue should be (0)
        `LIST`.lPushX("B").futureValue should be (0)
        `LIST`.lRange().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lPushX("A").!
        }
      }
    }
    "the list contains some elements" should {
      "prepend the value to the existing list" taggedAs V220 in {
        `LIST`.lPush("3")
        `LIST`.lPushX("2").futureValue should be (2)
        `LIST`.lPushX("1").futureValue should be (3)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "1", "2", "3"
        )
        `LIST`.del()
      }
    }
  }

  LRange.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        `LIST`.lRange().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lRange().!
        }
      }
    }
    "the list contains some elements" should {
      "return the elements in the specified range" taggedAs V100 in {
        `LIST`.rPush("0")
        `LIST`.rPush("1")
        `LIST`.rPush("2")
        `LIST`.rPush("3")
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        `LIST`.lRange(0, -1).futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        `LIST`.lRange(0, 0).futureValue should contain theSameElementsInOrderAs List("0")
        `LIST`.lRange(1, 1).futureValue should contain theSameElementsInOrderAs List("1")
        `LIST`.lRange(2, 2).futureValue should contain theSameElementsInOrderAs List("2")
        `LIST`.lRange(3, 3).futureValue should contain theSameElementsInOrderAs List("3")
        `LIST`.lRange(1, 2).futureValue should contain theSameElementsInOrderAs List(
          "1", "2"
        )
        `LIST`.lRange(-2, -1).futureValue should contain theSameElementsInOrderAs List(
          "2", "3"
        )
        `LIST`.lRange(-1, -1).futureValue should contain theSameElementsInOrderAs List("3")
        `LIST`.lRange(-4, -1).futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        `LIST`.del()
      }
    }
  }

  LRem.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V100 in {
        `LIST`.lRem("A").futureValue should be (0)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lRem("A").!
        }
      }
    }
    "the list contains some elements" should {
      "delete the specified elements" taggedAs V100 in {
        `LIST`.rPush("A")
        `LIST`.rPush("B")
        `LIST`.rPush("C")
        `LIST`.rPush("D")
        `LIST`.rPush("A")
        `LIST`.rPush("B")
        `LIST`.rPush("C")
        `LIST`.rPush("D")

        `LIST`.lRem("X").futureValue should be (0)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "A", "B", "C", "D"
        )

        `LIST`.lRem("A").futureValue should be (2)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "B", "C", "D", "B", "C", "D"
        )

        `LIST`.lRem("B", 1).futureValue should be (1)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "C", "D", "B", "C", "D"
        )

        `LIST`.lRem("C", -1).futureValue should be (1)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "C", "D", "B", "D"
        )

        `LIST`.lRem("D", -2).futureValue should be (2)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("C", "B")

        `LIST`.lRem("C", 50).futureValue should be (1)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("B")

        `LIST`.lRem("B", -50).futureValue should be (1)
        `LIST`.lRange().futureValue should be (empty)
      }
    }
  }

  LSet.toString when {
    "the key does not exist" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.lSet(0, "A").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.lSet(1, "A").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.lSet(-1, "A").!
        }
        `LIST`.lRange().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lSet(0, "A").!
        }
      }
    }
    "the list contains some elements" should {
      Given("the index is out of range")
      "return an error" taggedAs V100 in {
        `LIST`.rPush("A")
        `LIST`.rPush("B")
        `LIST`.rPush("C")
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.lSet(3, "X").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.lSet(-4, "X").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.lSet(55, "X").!
        }
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C"
        )
      }
      Given("the index is correct")
      "set the provided values at the corresponding indices" taggedAs V100 in {
        `LIST`.lSet(0, "D")
        `LIST`.lSet(1, "E")
        `LIST`.lSet(2, "F")
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "D", "E", "F"
        )
        `LIST`.lSet(-3, "A")
        `LIST`.lSet(-2, "B")
        `LIST`.lSet(-1, "C")
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C"
        )
        `LIST`.del()
      }
    }
  }

  LTrim.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs V100 in {
        `LIST`.lTrim(0, -1)
        `LIST`.lRange().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.lTrim(0, -1).!
        }
      }
    }
    "the list contains some elements" should {
      "trim the list to the specified range" taggedAs V100 in {
        `LIST`.rPush("0")
        `LIST`.rPush("1")
        `LIST`.rPush("2")
        `LIST`.rPush("3")

        `LIST2`.rPush("0")
        `LIST2`.rPush("1")
        `LIST2`.rPush("2")
        `LIST2`.rPush("3")

        `LIST3`.rPush("0")
        `LIST3`.rPush("1")
        `LIST3`.rPush("2")
        `LIST3`.rPush("3")

        `LIST`.lTrim(0, 0)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("0")
        `LIST2`.lTrim(0, -1)
        `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        `LIST2`.lTrim(-4, -1)
        `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        `LIST2`.lTrim(0, -3)
        `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List("0", "1")
        `LIST3`.lTrim(2, 3)
        `LIST3`.lRange().futureValue should contain theSameElementsInOrderAs List("2", "3")
        `LIST3`.lTrim(1, 1)
        `LIST3`.lRange().futureValue should contain theSameElementsInOrderAs List("3")
        `LIST`.del()
        `LIST2`.del()
        `LIST3`.del()
      }
    }
  }
  
  RPop.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        `LIST`.rPop().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { `HASH`.rPop().! }
      }
    }
    "the list contains some elements" should {
      "pop the first element of the list" taggedAs V100 in {
        `LIST`.rPush("A")
        `LIST`.rPush("B")
        `LIST`.rPop().futureValue should contain ("B")
        `LIST`.rPop().futureValue should contain ("A")
        `LIST`.rPop().futureValue should be (empty)
      }
    }
  }

  RPopLPush.toString when {
    "the source key does not exist" should {
      "return None and do nothing" taggedAs V120 in {
        `LIST`.rPopLPush(`LIST2`).futureValue should be (empty)
        `LIST`.lLen.futureValue should be (0)
        `LIST2`.lLen.futureValue should be (0)
      }
    }
    "the source and dest keys are identical" should {
      "do nothing" taggedAs V120 in {
        `LIST`.rPush("A")
        `LIST`.rPopLPush(`LIST`).futureValue should contain ("A")
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("A")
      }
    }
    "the dest key does not exist" should {
      "create a list at dest key and prepend the popped value" taggedAs V120 in {
        `LIST`.rPopLPush(`LIST2`).futureValue should contain ("A")
        `LIST`.lLen.futureValue should be (0)
        `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List("A")
      }
    }
    "one of the keys does not contain a list" should {
      "return an error" taggedAs V120 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.rPopLPush(`LIST`).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          `LIST2`.rPopLPush(`HASH`).!
        }
      }
    }
    "both lists contain some elements" should {
      "pop the last element of list at source key and " +
        "prepend it to list at dest key" taggedAs V120 in {
          `LIST`.rPush("A")
          `LIST`.rPush("B")
          `LIST`.rPush("C")

          `LIST`.rPopLPush(`LIST2`).futureValue should contain ("C")
          `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("A", "B")
          `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List("C", "A")

          `LIST`.rPopLPush(`LIST2`).futureValue should contain ("B")
          `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("A")
          `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List(
            "B", "C", "A"
          )

          `LIST`.rPopLPush(`LIST2`).futureValue should contain ("A")
          `LIST`.lRange().futureValue should be (empty)
          `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List(
            "A", "B", "C", "A"
          )

          `LIST2`.rPopLPush(`LIST`).futureValue should contain ("A")
          `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("A")
          `LIST2`.lRange().futureValue should contain theSameElementsInOrderAs List(
            "A", "B", "C"
          )

          `LIST`.del()
          `LIST2`.del()
        }
    }
  }
  
  RPush.toString when {
    "the key does not exist" should {
      "create a list and append the value" taggedAs V100 in {
        `LIST`.rPush("A").futureValue should be (1)
        `LIST`.rPush("B").futureValue should be (2)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("A", "B")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.rPush("A").!
        }
      }
    }
    "the list contains some elements" should {
      "append the value to the existing list" taggedAs V100 in {
        `LIST`.rPush("1").futureValue should be (3)
        `LIST`.rPush("2").futureValue should be (4)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "1", "2"
        )
        `LIST`.del()
      }
    }
  }

  s"${RPush.toString}-2.4" when {
    "the key does not exist" should {
      "create a list and append the values" taggedAs V240 in {
        `LIST`.rPush("A", "B").futureValue should be (2)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List("A", "B")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V240 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.rPush("A", "B").!
        }
      }
    }
    "the list contains some elements" should {
      "append the values to the existing list" taggedAs V240 in {
        `LIST`.rPush("1", "2").futureValue should be (4)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "1", "2"
        )
        `LIST`.del()
      }
    }
  }
  
  RPushX.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs V220 in {
        `LIST`.rPushX("A").futureValue should be (0)
        `LIST`.rPushX("B").futureValue should be (0)
        `LIST`.lRange().futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `HASH`.rPushX("A").!
        }
      }
    }
    "the list contains some elements" should {
      "append the value to the existing list" taggedAs V220 in {
        `LIST`.rPush("1")
        `LIST`.rPushX("2").futureValue should be (2)
        `LIST`.rPushX("3").futureValue should be (3)
        `LIST`.lRange().futureValue should contain theSameElementsInOrderAs List(
          "1", "2", "3"
        )
        `LIST`.del()
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }

}
