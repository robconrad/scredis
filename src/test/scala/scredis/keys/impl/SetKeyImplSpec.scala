package scredis.keys.impl

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions._
import scredis.protocol.requests.SetRequests._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer

class SetKeyImplSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {

  private implicit val client = Client()

  private val keyPrefix = "keyPrefix"

  private def makeKey(myKey: String) =
    new SetKeyImpl[String, String, String](keyPrefix, myKey)
  
  private val setKey = makeKey("SET")
  private val setKey1 = makeKey("SET1")
  private val setKey2 = makeKey("SET2")
  private val setKey3 = makeKey("SET3")
  private val setKeyScan = makeKey("SSET")
  private val setKeyHash = makeKey("HASH")
  
  private val SomeValue = "HelloWorld!虫àéç蟲"

  private val MEMBER = true
  private val NOT_MEMBER = false

  override def beforeAll(): Unit = {
    client.hSet(keyPrefix + "HASH", "FIELD", SomeValue).!
  }
  
  SAdd.toString when {
    "the key does not exist" should {
      "create a set and add the member to it" taggedAs V100 in {
        setKey.add(SomeValue).futureValue should be(1)
        setKey.members.futureValue should contain theSameElementsAs List(SomeValue)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a[RedisErrorResponseException] should be thrownBy {
          setKeyHash.add("hello").!
        }
      }
    }
    "the set contains some elements" should {
      "add the provided member only if it is not already contained in the set" taggedAs V100 in {
        setKey.add(SomeValue).futureValue should be(0)
        setKey.members.futureValue should contain theSameElementsAs List(SomeValue)
        setKey.add("A").futureValue should be(1)
        setKey.members.futureValue should contain theSameElementsAs List(SomeValue, "A")
        setKey.del()
      }
    }
  }
  
  s"${SAdd.toString}-2.4" when {
    "the key does not exist" should {
      "create a set and add the members to it" taggedAs V240 in {
        setKey.add(SomeValue, "A").futureValue should be (2)
        setKey.members.futureValue should contain theSameElementsAs List(SomeValue, "A")
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V240 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.add("hello", "asd").!
        }
      }
    }
    "the set contains some elements" should {
      "add the provided members only if it is not already contained in the set" taggedAs V240 in {
        setKey.add(SomeValue, "A").futureValue should be (0)
        setKey.members.futureValue should contain theSameElementsAs List(SomeValue, "A")
        setKey.add("B", "C").futureValue should be (2)
        setKey.members.futureValue should contain theSameElementsAs List(
          SomeValue, "A", "B", "C"
        )
        setKey.del()
      }
    }
  }

  SCard.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V100 in {
        setKey.card.futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.card.!
        }
      }
    }
    "the set contains some elements" should {
      "return the number of element in the set" taggedAs V100 in {
        setKey.add("1")
        setKey.add("2")
        setKey.add("3")
        setKey.card.futureValue should be (3)
        setKey.del()
      }
    }
  }

  SDiff.toString when {
    "all keys do not exist" should {
      "return None" taggedAs V100 in {
        setKey1.diff(setKey2, setKey3).futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs V100 in {
        setKey1.add("A")
        setKey1.diff(setKey2, setKey3).futureValue should contain theSameElementsAs List("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.diff(setKey1, setKey2).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          setKey1.diff(setKeyHash, setKey2).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          setKey1.diff(setKey2, setKeyHash).!
        }
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs V100 in {
        setKey1.add("B")
        setKey1.add("C")
        setKey1.add("D")

        setKey2.add("C")

        setKey3.add("A")
        setKey3.add("C")
        setKey3.add("E")

        setKey1.diff(setKey1).futureValue should be (empty)
        setKey1.diff(setKey2).futureValue should contain theSameElementsAs List(
          "A", "B", "D"
        )
        setKey1.diff(setKey3).futureValue should contain theSameElementsAs List(
          "B", "D"
        )
        setKey1.diff(setKey2, setKey3).futureValue should contain theSameElementsAs List(
          "B", "D"
        )
        setKey1.del()
        setKey2.del()
        setKey3.del()
      }
    }
  }

  SDiffStore.toString when {
    "all keys do not exist" should {
      "return None" taggedAs V100 in {
        setKey.diffStore(setKey1, setKey2, setKey3).futureValue should be (0)
        setKey.card.futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs V100 in {
        setKey1.add("A")
        setKey.diffStore(setKey1, setKey2, setKey3).futureValue should be (1)
        setKey.members.futureValue should contain theSameElementsAs List("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.diffStore(setKeyHash, setKey2, setKey3).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.diffStore(setKey1, setKeyHash, setKey3).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.diffStore(setKey1, setKey2, setKeyHash).!
        }
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs V100 in {
        setKey1.add("B")
        setKey1.add("C")
        setKey1.add("D")

        setKey2.add("C")

        setKey3.add("A")
        setKey3.add("C")
        setKey3.add("E")

        setKey.diffStore(setKey1, setKey1).futureValue should be (0)
        setKey.members.futureValue should be (empty)

        setKey.diffStore(setKey1, setKey2).futureValue should be (3)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "B", "D"
        )

        setKey.diffStore(setKey1, setKey3).futureValue should be (2)
        setKey.members.futureValue should contain theSameElementsAs List(
          "B", "D"
        )

        setKey.diffStore(setKey1, setKey2, setKey3).futureValue should be (2)
        setKey.members.futureValue should contain theSameElementsAs List(
          "B", "D"
        )
        setKey.del()
        setKey1.del()
        setKey2.del()
        setKey3.del()
      }
    }
  }

  SInter.toString when {
    "all keys do not exist" should {
      "return None" taggedAs V100 in {
        setKey1.inter(setKey2, setKey3).futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs V100 in {
        setKey1.add("A")
        setKey1.inter(setKey2, setKey3).futureValue should be (empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.inter(setKey1, setKey2).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          setKey1.inter(setKeyHash, setKey2).!
        }
        setKey2.add("A")
        a [RedisErrorResponseException] should be thrownBy {
          setKey1.inter(setKey2, setKeyHash).!
        }
        setKey2.del()
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs V100 in {
        setKey1.add("B")
        setKey1.add("C")
        setKey1.add("D")

        setKey2.add("C")

        setKey3.add("A")
        setKey3.add("C")
        setKey3.add("E")

        setKey1.inter(setKey1).futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )
        setKey1.inter(setKey2).futureValue should contain theSameElementsAs List(
          "C"
        )
        setKey1.inter(setKey3).futureValue should contain theSameElementsAs List(
          "A", "C"
        )
        setKey1.inter(setKey2, setKey3).futureValue should contain theSameElementsAs List(
          "C"
        )
        setKey1.del()
        setKey2.del()
        setKey3.del()
      }
    }
  }

  SInterStore.toString when {
    "all keys do not exist" should {
      "return None" taggedAs V100 in {
        setKey.interStore(setKey1, setKey2, setKey3).futureValue should be (0)
        setKey.card.futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs V100 in {
        setKey1.add("A")
        setKey.interStore(setKey1, setKey2, setKey3).futureValue should be (0)
        setKey.members.futureValue should be (empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.interStore(setKeyHash, setKey2, setKey3).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.interStore(setKey1, setKeyHash, setKey3).!
        }
        setKey2.add("A")
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.interStore(setKey1, setKey2, setKeyHash).!
        }
        setKey2.del()
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs V100 in {
        setKey1.add("B")
        setKey1.add("C")
        setKey1.add("D")

        setKey2.add("C")

        setKey3.add("A")
        setKey3.add("C")
        setKey3.add("E")

        setKey.interStore(setKey1, setKey1).futureValue should be (4)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )

        setKey.interStore(setKey1, setKey2).futureValue should be (1)
        setKey.members.futureValue should contain theSameElementsAs List(
          "C"
        )

        setKey.interStore(setKey1, setKey3).futureValue should be (2)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "C"
        )

        setKey.interStore(setKey1, setKey2, setKey3).futureValue should be (1)
        setKey.members.futureValue should contain theSameElementsAs List(
          "C"
        )
        setKey.del()
        setKey1.del()
        setKey2.del()
        setKey3.del()
      }
    }
  }

  SIsMember.toString when {
    "the key does not exist" should {
      "return false" taggedAs V100 in {
        setKey.isMember("A").futureValue should be (NOT_MEMBER)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.isMember("A").!
        }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs V100 in {
        setKey.add("1")
        setKey.add("2")
        setKey.add("3")
        setKey.isMember("A").futureValue should be (NOT_MEMBER)
        setKey.isMember("1").futureValue should be (MEMBER)
        setKey.isMember("2").futureValue should be (MEMBER)
        setKey.isMember("3").futureValue should be (MEMBER)
        setKey.del()
      }
    }
  }

  SMembers.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        setKey.members.futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.members.!
        }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs V100 in {
        setKey.add("1")
        setKey.add("2")
        setKey.add("3")
        setKey.members.futureValue should contain theSameElementsAs List("1", "2", "3")
        setKey.del()
      }
    }
  }

  SMove.toString when {
    "the key does not exist" should {
      "return false" taggedAs V100 in {
        setKey1.move(setKey2, "A").futureValue should be (NOT_MEMBER)
        setKey2.isMember("A").futureValue should be (NOT_MEMBER)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.move(setKey, "A").!
        }
        setKey.add("A")
        a [RedisErrorResponseException] should be thrownBy {
          setKey.move(setKeyHash, "A").!
        }
        setKey.del()
      }
    }
    "the set contains some elements" should {
      "move the member from one set to another" taggedAs V100 in {
        setKey1.add("A")
        setKey1.add("B")
        setKey1.add("C")

        setKey1.move(setKey2, "B").futureValue should be (MEMBER)
        setKey1.members.futureValue should contain theSameElementsAs List("A", "C")
        setKey2.isMember("B").futureValue should be (MEMBER)
        setKey1.del()
        setKey2.del()
      }
    }
  }

  SPop.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        setKey.pop().futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.pop().!
        }
      }
    }
    "the set contains some elements" should {
      "return a random member and remove it" taggedAs V100 in {
        setKey.add("A")
        setKey.add("B")
        setKey.add("C")

        val member1 = setKey.pop().futureValue
        member1 should contain oneOf ("A", "B", "C")
        setKey.isMember(member1.get).futureValue should be (NOT_MEMBER)

        val member2 = setKey.pop().futureValue
        member2 should contain oneOf ("A", "B", "C")
        member2 should not contain member1
        setKey.isMember(member2.get).futureValue should be (NOT_MEMBER)

        val member3 = setKey.pop().futureValue
        member3 should contain oneOf ("A", "B", "C")
        member3 should not contain member1
        member3 should not contain member2
        setKey.isMember(member3.get).futureValue should be (NOT_MEMBER)

        setKey.pop().futureValue should be (empty)
      }
    }
  }

  SRandMember.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        setKey.randMember.futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.randMember.!
        }
      }
    }
    "the set contains some elements" should {
      "return a random member but do not remove it" taggedAs V100 in {
        setKey.add("A")
        setKey.add("B")
        setKey.add("C")

        setKey.randMember.futureValue should contain oneOf ("A", "B", "C")
        setKey.card.futureValue should be (3)
        setKey.del()
      }
    }
  }
  
  s"${SRandMember.toString}-2.6" when {
    "the key does not exist" should {
      "return None" taggedAs V260 in {
        setKey.randMembers(3).futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V260 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.randMembers(3).!
        }
      }
    }
    "the set contains some elements" should {
      "return count random members and do not remove them" taggedAs V260 in {
        setKey.add("A")
        setKey.add("B")
        setKey.add("C")
        setKey.add("D")

        val members = setKey.randMembers(3).futureValue
        members should have size 3
        setKey.card.futureValue should be (4)
        setKey.del()
      }
    }
  }

  SRem.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V100 in {
        setKey.rem("A").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.rem("A").!
        }
      }
    }
    "the set contains some elements" should {
      "remove the member and return 1" taggedAs V100 in {
        setKey.add("A")
        setKey.add("B")
        setKey.add("C")

        setKey.rem("B").futureValue should be (1)
        setKey.members.futureValue should contain theSameElementsAs List("A", "C")

        setKey.rem("B").futureValue should be (0)
        setKey.members.futureValue should contain theSameElementsAs List("A", "C")

        setKey.rem("A").futureValue should be (1)
        setKey.members.futureValue should contain theSameElementsAs List("C")

        setKey.rem("C").futureValue should be (1)
        setKey.members.futureValue should be (empty)
      }
    }
  }

  s"${SRem.toString}-2.4" when {
    "the key does not exist" should {
      "return 0" taggedAs V240 in {
        setKey.rem("A", "B").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V240 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.rem("A", "B").!
        }
      }
    }
    "the set contains some elements" should {
      "remove the members and return the number of members that were removed" taggedAs V240 in {
        setKey.add("A")
        setKey.add("B")
        setKey.add("C")

        setKey.rem("B", "C").futureValue should be (2)
        setKey.members.futureValue should contain theSameElementsAs List("A")

        setKey.rem("A", "B", "C").futureValue should be (1)
        setKey.members.futureValue should be (empty)

        setKey.rem("A", "B", "C").futureValue should be (0)
      }
    }
  }
  
  SScan.toString when {
    "the key does not exist" should {
      "return an empty set" taggedAs V280 in {
        val (next, set) = makeKey("NONEXISTENTKEY").scan(0).!
        next should be (0)
        set should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs V280 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.scan(0).!
        }
      }
    }
    "the set contains 5 elements" should {
      "return all elements" taggedAs V280 in {
        for (i <- 1 to 5) {
          setKeyScan.add("value" + i)
        }
        val (next, set) = setKeyScan.scan(0).!
        next should be (0)
        set should contain theSameElementsAs List("value1", "value2", "value3", "value4", "value5")
        for (i <- 1 to 10) {
          setKeyScan.add("foo" + i)
        }
      }
    }
    "the set contains 15 elements" should {
      val full = ListBuffer[String]()
      for (i <- 1 to 5) {
        full += ("value" + i)
      }
      for (i <- 1 to 10) {
        full += ("foo" + i)
      }
      val fullList = full.toList
      
      Given("that no pattern is set")
      "return all elements" taggedAs V280 in {
        val elements = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = setKeyScan.scan(cursor).!
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsAs fullList
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs V280 in {
        val elements = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = setKeyScan.scan(cursor, matchOpt = Some("foo*")).!
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching elements in one iteration" taggedAs V280 in {
        val elements = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = setKeyScan.scan(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size should be (10)
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
    }
  }
  
  SUnion.toString when {
    "all keys do not exist" should {
      "return None" taggedAs V100 in {
        setKey1.union(setKey2, setKey3).futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs V100 in {
        setKey1.add("A")
        setKey1.union(setKey2, setKey3).futureValue should contain theSameElementsAs List(
          "A"
        )
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          setKeyHash.union(setKey1, setKey2).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          setKey1.union(setKeyHash, setKey2).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          setKey1.union(setKey2, setKeyHash).!
        }
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs V100 in {
        setKey1.add("B")
        setKey1.add("C")
        setKey1.add("D")

        setKey2.add("C")

        setKey3.add("A")
        setKey3.add("C")
        setKey3.add("E")

        setKey1.union(setKey1).futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )
        setKey1.union(setKey2).futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )
        setKey1.union(setKey3).futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )
        setKey1.union(setKey2, setKey3).futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )
        setKey1.del()
        setKey2.del()
        setKey3.del()
      }
    }
  }

  SUnionStore.toString when {
    "all keys do not exist" should {
      "return None" taggedAs V100 in {
        setKey.unionStore(setKey1, setKey2, setKey3).futureValue should be (0)
        setKey.card.futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs V100 in {
        setKey1.add("A")
        setKey.unionStore(setKey1, setKey2, setKey3).futureValue should be (1)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A"
        )
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.unionStore(setKeyHash, setKey2, setKey3).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.unionStore(setKey1, setKeyHash, setKey3).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          setKey.unionStore(setKey1, setKey2, setKeyHash).!
        }
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs V100 in {
        setKey1.add("B")
        setKey1.add("C")
        setKey1.add("D")

        setKey2.add("C")

        setKey3.add("A")
        setKey3.add("C")
        setKey3.add("E")

        setKey.unionStore(setKey1, setKey1).futureValue should be (4)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )

        setKey.unionStore(setKey1, setKey2).futureValue should be (4)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )

        setKey.unionStore(setKey1, setKey3).futureValue should be (5)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )

        setKey.unionStore(setKey1, setKey2, setKey3).futureValue should be (5)
        setKey.members.futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )
        setKey.del()
        setKey1.del()
        setKey2.del()
        setKey3.del()
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }

}
