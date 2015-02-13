package scredis.keys.impl

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions._
import scredis.keys.SimpleKey
import scredis.protocol.requests.KeyRequests._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer

class KeyImplSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with EitherValues
  with ScalaFutures {
  
  private implicit val client = Client()

  private val keyPrefix = "keyPrefix"

  // uses a SimpleKeyImpl so we can do .set to create the key, otherwise this is only testing KeyImpl commands
  private def makeKey(myKey: String): SimpleKey[String, String, String] =
    new SimpleKeyImpl[String, String, String](keyPrefix, myKey)

  private val `destKey` = makeKey("destKey")
  private val `HASH` = makeKey("HASH")
  private val `I-EXIST-2` = makeKey("I-EXIST-2")
  private val `I-EXIST-3` = makeKey("I-EXIST-3")
  private val `I-EXIST` = makeKey("I-EXIST")
  private val `LIST` = makeKey("LIST")
  private val `NON-EXISTENT-KEY` = makeKey("NON-EXISTENT-KEY")
  private val `OBJECT-ENCODING` = makeKey("OBJECT-ENCODING")
  private val `OBJECT-IDLE-TIME` = makeKey("OBJECT-IDLE-TIME")
  private val `OBJECT-REF-COUNT` = makeKey("OBJECT-REF-COUNT")
  private val `SET` = makeKey("SET")
  private val `SORTED-SET` = makeKey("SORTED-SET")
  private val `sourceKey` = makeKey("sourceKey")
  private val `STRING` = makeKey("STRING")
  private val `TO-DUMP` = makeKey("TO-DUMP")
  private val `TO-EXPIRE` = makeKey("TO-EXPIRE")
  private val `TO-MOVE` = makeKey("TO-MOVE")
  private val `TO-PERSIST` = makeKey("TO-PERSIST")
  private val `TO-TTL` = makeKey("TO-TTL")

  private val SomeValue = "HelloWorld!虫àéç蟲"
  
  private val EXISTS = true
  private val NOT_EXISTS = false

  private val PERSISTED = true
  private val TTL_SET = true
  private val TTL_NOT_SET = false

  private val MOVED = true
  private val NOT_MOVED = false

  private val RENAMED = true
  private val NOT_RENAMED = false

  private val DELETED = true
  private val NOT_DELETED = false

  private var dumpedValue: Array[Byte] = _

  Del.toString when {
    "deleting a single key that does not exist" should {
      "return 0" taggedAs V100 in {
        `NON-EXISTENT-KEY`.del().futureValue should be (NOT_DELETED)
      }
    }
    "deleting a single key that exists" should {
      "delete the key" taggedAs V100 in {
        `I-EXIST`.set("YES")
        `I-EXIST`.del().futureValue should be (DELETED)
        `I-EXIST`.del().futureValue should be (NOT_DELETED)
      }
    }
    "deleting multiple keys that partially exist" should {
      "delete the existing keys" taggedAs V100 in {
        `I-EXIST`.set("YES")
        `I-EXIST-2`.set("YES")
        `I-EXIST`.del().futureValue should be (DELETED)
        `I-EXIST-2`.del().futureValue should be (DELETED)
        `I-EXIST-3`.del().futureValue should be (NOT_DELETED)
        `I-EXIST`.del().futureValue should be (NOT_DELETED)
        `I-EXIST-2`.del().futureValue should be (NOT_DELETED)
      }
    }
  }

  Dump.toString when {
    "the key does not exist" should {
      "return None" taggedAs V260 in {
        `NON-EXISTENT-KEY`.dump.futureValue should be (empty)
      }
    }
    "the key exists" should {
      "return the serialized value for that key" taggedAs V260 in {
        `TO-DUMP`.set(SomeValue)
        val dump = `TO-DUMP`.dump.!
        dump should be (defined)
        dumpedValue = dump.get
      }
    }
  }

  Exists.toString when {
    "the key does not exist" should {
      "return false" taggedAs V100 in {
        `NON-EXISTENT-KEY`.exists.futureValue should be (NOT_EXISTS)
      }
    }
    "the key exists" should {
      "return true" taggedAs V100 in {
        `TO-DUMP`.exists.futureValue should be (EXISTS)
      }
    }
  }

  Expire.toString when {
    "the key does not exist" should {
      "return false" taggedAs V100 in {
        `NON-EXISTENT-KEY`.expire(1).futureValue should be (TTL_NOT_SET)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs V100 in {
        `TO-EXPIRE`.set("HEY")
        `TO-EXPIRE`.expire(1).futureValue should be (TTL_SET)
        `TO-EXPIRE`.get.futureValue should be (defined)
        Thread.sleep(1050)
        `TO-EXPIRE`.get.futureValue should be (empty)
      }
    }
  }

  ExpireAt.toString when {
    "the key does not exist" should {
      "return false" taggedAs V120 in {
        val unixTimestamp = System.currentTimeMillis / 1000
        `NON-EXISTENT-KEY`.expireAt(unixTimestamp).futureValue should be (TTL_NOT_SET)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs V120 in {
        val unixTimestamp = System.currentTimeMillis / 1000 + 2
        `TO-EXPIRE`.set("HEY")
        `TO-EXPIRE`.expireAt(unixTimestamp).futureValue should be (TTL_SET)
        `TO-EXPIRE`.get.futureValue should be (defined)
        Thread.sleep(2050)
        `TO-EXPIRE`.get.futureValue should be (empty)
      }
    }
  }

  Move.toString when {
    "moving a key that does not exist" should {
      "return false" taggedAs V100 in {
        `NON-EXISTENT-KEY`.move(1).futureValue should be (NOT_MOVED)
      }
    }
    "moving a key from database 0 to 1" should {
      Given("that the key does not exist in database 1 yet")
      "succeed" taggedAs V100 in {
        `TO-MOVE`.set(SomeValue).futureValue
        `TO-MOVE`.move(1).futureValue should be (MOVED)
        `TO-MOVE`.exists.futureValue should be (NOT_EXISTS)
        client.select(1).futureValue
        `TO-MOVE`.get.futureValue should contain (SomeValue)
      }
      Given("that the key already exists in database 1")
      "fail" taggedAs V100 in {
        client.select(0).futureValue
        `TO-MOVE`.set(SomeValue).futureValue
        `TO-MOVE`.move(1).futureValue should be (NOT_MOVED)
        `TO-MOVE`.exists.futureValue should be (EXISTS)
        client.flushAll().futureValue
      }
    }
  }
  
  ObjectRefCount.toString when {
    "the object does not exist" should {
      "return None" taggedAs V223 in {
        `NON-EXISTENT-KEY`.objectRefCount.futureValue should be (empty)
      }
    }
    "the object exists" should {
      "return its ref count" taggedAs V223 in {
        `OBJECT-REF-COUNT`.set(SomeValue)
        `OBJECT-REF-COUNT`.objectRefCount.futureValue should be (defined)
      }
    }
  }
  
  ObjectEncoding.toString when {
    "the object does not exist" should {
      "return None" taggedAs V223 in {
        `NON-EXISTENT-KEY`.objectEncoding.futureValue should be (empty)
      }
    }
    "the object exists" should {
      "return its encoding" taggedAs V223 in {
        `OBJECT-ENCODING`.set(SomeValue)
        `OBJECT-ENCODING`.objectEncoding.futureValue should be (defined)
      }
    }
  }
  
  ObjectIdleTime.toString when {
    "the object does not exist" should {
      "return None" taggedAs V223 in {
        `NON-EXISTENT-KEY`.objectIdleTime.futureValue should be (empty)
      }
    }
    "the object exists" should {
      "return its idle time" taggedAs V223 in {
        `OBJECT-IDLE-TIME`.set(SomeValue)
        `OBJECT-IDLE-TIME`.objectIdleTime.futureValue should be (defined)
      }
    }
  }

  Persist.toString when {
    "persisting a non-existent key" should {
      "return false" taggedAs V220 in {
        `NON-EXISTENT-KEY`.persist().futureValue should be (TTL_NOT_SET)
      }
    }
    "persisting a key that has no ttl" should {
      "return false" taggedAs V220 in {
        `TO-PERSIST`.set(SomeValue)
        `TO-PERSIST`.persist().futureValue should be (TTL_NOT_SET)
      }
    }
    "persisting a key that has a ttl" should {
      "return true and the key should not expire" taggedAs V220 in {
        `TO-PERSIST`.pExpire(500)
        `TO-PERSIST`.persist().futureValue should be (PERSISTED)
        Thread.sleep(550)
        `TO-PERSIST`.exists.futureValue should be (EXISTS)
      }
    }
  }
  
  PExpire.toString when {
    "the key does not exist" should {
      "return false" taggedAs V260 in {
        `NON-EXISTENT-KEY`.pExpire(500).futureValue should be (TTL_NOT_SET)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs V260 in {
        `TO-EXPIRE`.set("HEY")
        `TO-EXPIRE`.pExpire(500).futureValue should be (TTL_SET)
        `TO-EXPIRE`.get.futureValue should be (defined)
        Thread.sleep(550)
        `TO-EXPIRE`.get.futureValue should be (empty)
      }
    }
  }

  PExpireAt.toString when {
    "the key does not exist" should {
      "return false" taggedAs V260 in {
        val unixTimestampMillis = System.currentTimeMillis + 500
        `NON-EXISTENT-KEY`.pExpireAt(unixTimestampMillis).futureValue should be (TTL_NOT_SET)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs V260 in {
        val unixTimestampMillis = System.currentTimeMillis + 500
        `TO-EXPIRE`.set("HEY")
        `TO-EXPIRE`.pExpireAt(unixTimestampMillis).futureValue should be (TTL_SET)
        `TO-EXPIRE`.get.futureValue should be (defined)
        Thread.sleep(550)
        `TO-EXPIRE`.get.futureValue should be (empty)
      }
    }
  }
  
  PTTL.toString when {
    "key does not exist" should {
      "return Left(false)" taggedAs V260 in {
        `NON-EXISTENT-KEY`.pTtl.futureValue should be (Left(false))
      }
    }
    "key exists but has no ttl" should {
      "return Left(true)" taggedAs V280 in {
        `TO-TTL`.set(SomeValue)
        `TO-TTL`.pTtl.futureValue should be (Left(true))
      }
    }
    "key exists and has a ttl" should {
      "return None" taggedAs V260 in {
        `TO-TTL`.pExpire(500)
        `TO-TTL`.pTtl.futureValue.right.value should be <= 500L
        `TO-TTL`.del()
      }
    }
  }

  RandomKey.toString when {
    "the database is empty" should {
      "return None" taggedAs V100 in {
        client.flushDB()
        client.randomKey().futureValue should be (empty)
      }
    }
    "the database has some keys" should {
      "return None" taggedAs V100 in {
        client.set("key1", "value1")
        client.set("key2", "value2")
        client.set("key3", "value3")
        client.randomKey().futureValue should contain oneOf ("key1", "key2", "key3")
      }
    }
  }

  Rename.toString when {
    "the key does not exist" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy { 
          `sourceKey`.rename(`destKey`).!
        }
      }
    }
    "the source key exists but destination key is identical to source key" should {
      "return an error" taggedAs V100 in {
        `sourceKey`.set(SomeValue)
        a [RedisErrorResponseException] should be thrownBy { 
          `sourceKey`.rename(`sourceKey`).!
        }
      }
    }
    "the source key exists and destination key is different" should {
      "succeed" taggedAs V100 in {
        `sourceKey`.rename(`destKey`)
        `sourceKey`.exists.futureValue should be (NOT_EXISTS)
        `destKey`.get.futureValue should contain (SomeValue)
      }
    }
    "the source key exists and destination key is different but already exists" should {
      "succeed and overwrite destKey" taggedAs V100 in {
        `sourceKey`.set("OTHERVALUE")
        `sourceKey`.rename(`destKey`)
        `sourceKey`.exists.futureValue should be (NOT_EXISTS)
        `destKey`.get.futureValue should contain ("OTHERVALUE")
      }
    }
  }

  RenameNX.toString when {
    "the key does not exist" should {
      "return an error" taggedAs V100 in {
        `sourceKey`.del()
        `destKey`.del()
        a [RedisErrorResponseException] should be thrownBy { 
          `sourceKey`.renameNX(`destKey`).!
        }
      }
    }
    "the source key exists but destination key is identical to source key" should {
      "return an error" taggedAs V100 in {
        `sourceKey`.set(SomeValue)
        a [RedisErrorResponseException] should be thrownBy { 
          `sourceKey`.renameNX(`sourceKey`).!
        }
      }
    }
    "the source key exists and destination key is different" should {
      "succeed" taggedAs V100 in {
        `sourceKey`.renameNX(`destKey`).futureValue should be (RENAMED)
        `sourceKey`.exists.futureValue should be (NOT_EXISTS)
        `destKey`.get.futureValue should contain (SomeValue)
      }
    }
    "the source key exists and destination key is different but already exists" should {
      "return an error" taggedAs V100 in {
        `sourceKey`.set("OTHERVALUE")
        `sourceKey`.renameNX(`destKey`).futureValue should be (NOT_RENAMED)
      }
    }
  }

  Scan.toString when {
    "the database is empty" should {
      "return an empty set" taggedAs V280 in {
        client.flushDB().futureValue should be (())
        val (next, set) = client.scan(0).!
        next should be (0)
        set should be (empty)
      }
    }
    "the database contains 5 keys" should {
      "return all keys" taggedAs V280 in {
        for (i <- 1 to 5) {
          client.set("key" + i, SomeValue)
        }
        val (next, set) = client.scan(0).!
        next should be (0)
        set should contain theSameElementsAs List(
          "key1", "key2", "key3", "key4", "key5"
        )
        for (i <- 1 to 10) {
          client.set("foo" + i, SomeValue)
        }
      }
    }
    "the database contains 15 keys" should {
      val full = ListBuffer[String]()
      for (i <- 1 to 5) {
        full += ("key" + i)
      }
      for (i <- 1 to 10) {
        full += ("foo" + i)
      }
      val fullList = full.toList
      
      Given("that no pattern is set")
      "return all keys" taggedAs V280 in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(cursor).!
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList should contain theSameElementsAs fullList
      }
      Given("that a pattern is set")
      "return all matching keys" taggedAs V280 in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(cursor, matchOpt = Some("foo*")).!
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching keys in one iteration" taggedAs V280 in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size should be (10)
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
    }
  }

  TTL.toString when {
    "key does not exist" should {
      "return Left(false)" taggedAs V100 in {
        `NON-EXISTENT-KEY`.ttl.futureValue should be (Left(false))
      }
    }
    "key exists but has no ttl" should {
      "return Left(true)" taggedAs V280 in {
        `TO-TTL`.set(SomeValue)
        `TO-TTL`.ttl.futureValue should be (Left(true))
      }
    }
    "key exists and has a ttl" should {
      "return Right(ttl)" taggedAs V100 in {
        `TO-TTL`.expire(1)
        `TO-TTL`.ttl.futureValue.right.value should be <= 1
        `TO-TTL`.del()
      }
    }
  }

  scredis.protocol.requests.KeyRequests.Type.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        `NON-EXISTENT-KEY`.`type`.futureValue should be (empty)
      }
    }
    "the key is a string" should {
      "return string" taggedAs V100 in {
        `STRING`.set("HELLO")
        `STRING`.`type`.futureValue should contain (scredis.Type.String)
        `STRING`.del()
      }
    }
    "the key is a hash" should {
      "return hash" taggedAs V100 in {
        client.hSet(keyPrefix + "HASH", "FIELD", "VALUE")
        `HASH`.`type`.futureValue should contain (scredis.Type.Hash)
        `HASH`.del()
      }
    }
    "the key is a list" should {
      "return list" taggedAs V100 in {
        client.rPush(keyPrefix + "LIST", "HELLO")
        `LIST`.`type`.futureValue should contain (scredis.Type.List)
        `LIST`.del()
      }
    }
    "the key is a set" should {
      "return set" taggedAs V100 in {
        client.sAdd(keyPrefix + "SET", "HELLO")
        `SET`.`type`.futureValue should contain (scredis.Type.Set)
        `SET`.del()
      }
    }
    "the key is a sorted set" should {
      "return zset" taggedAs V100 in {
        client.zAdd(keyPrefix + "SORTED-SET", "HELLO", Score.Value(0))
        `SORTED-SET`.`type`.futureValue should contain (scredis.Type.SortedSet)
        `SORTED-SET`.del()
      }
    }
  }

  override def afterAll() {
    client.flushAll().!
    client.quit().!
  }

}