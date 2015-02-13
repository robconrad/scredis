package scredis.keys.impl

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions.RedisErrorResponseException
import scredis.keys.SimpleKey
import scredis.protocol.requests.StringRequests._
import scredis.serialization.Implicits._
import scredis.tags._
import scredis.util.TestUtils._

import scala.concurrent.duration._

class SimpleKeyImplSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private implicit val client = Client()

  private val keyPrefix = "keyPrefix"

  // uses a SimpleKeyImpl so we can do .set to create the key, otherwise this is only testing KeyImpl commands
  private def makeKey(myKey: String): SimpleKey[String, String, String] =
    new SimpleKeyImpl[String, String, String](keyPrefix, myKey)

  private val `BIT` = makeKey("BIT")
  private val `BOTH` = makeKey("BOTH")
  private val `DECR` = makeKey("DECR")
  private val `DOLLAR` = makeKey("DOLLAR")
  private val `I-EXIST` = makeKey("I-EXIST")
  private val `INCR` = makeKey("INCR")
  private val `KEY` = makeKey("KEY")
  private val `LIST` = makeKey("LIST")
  private val `NON-EXISTENT-KEY` = makeKey("NON-EXISTENT-KEY")
  private val `POUND` = makeKey("POUND")
  private val `STR` = makeKey("STR")
  private val `STRING` = makeKey("STRING")
  private val `TO-EXPIRE` = makeKey("TO-EXPIRE")

  private val SomeValue = "HelloWorld!虫àéç蟲"
  
  private val BIT_SET = true
  private val BIT_NOT_SET = false

  private val EXISTS = true
  private val NOT_EXISTS = false

  private val SET_CORRECTLY = true
  private val CONDITION_NOT_MET = false

  override def beforeAll() {
    client.lPush(keyPrefix + "LIST", "A").!
  }

  Append.toString when {
    "the key does not contain a string" should {
      "return an error" taggedAs V200 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.append("a").!
        }
      }
    }
    "appending the empty string" should {
      Given("that the key does not exist")
      "create an empty string" taggedAs V200 in {
        `NON-EXISTENT-KEY`.append("").futureValue should be (0)
        `NON-EXISTENT-KEY`.get.futureValue should contain ("")
      }
      Given("that the key exists and contains a string")
      "not modify the current value" taggedAs V200 in {
        `STR`.set("hello")
        `STR`.append("").futureValue should be (5)
        `STR`.get.futureValue should contain ("hello")
      }
    }
    "appending some string" should {
      Given("that the key does not exist")
      "append (and create) the string and return the correct size" taggedAs V200 in {
        `NON-EXISTENT-KEY`.append("hello").futureValue should be (5)
        `NON-EXISTENT-KEY`.get.futureValue should contain ("hello")
        `NON-EXISTENT-KEY`.del()
      }
      Given("that the key exists")
      "append the string and return the correct size" taggedAs V200 in {
        `STR`.set("hello")
        `STR`.append("world").futureValue should be (10)
        `STR`.get.futureValue should contain ("helloworld")
      }
    }
  }

  BitCount.toString when {
    "the key does not exists" should {
      "return 0" taggedAs V260 in {
        `NON-EXISTENT-KEY`.bitCount().futureValue should be (0)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs V260 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.bitCount().!
        }
      }
    }
    "the string is empty" should {
      "return 0" taggedAs V260 in {
        `STR`.set("")
        `STR`.bitCount().futureValue should be (0)
      }
    }
    "the string contains some bits set to 1" should {
      Given("that no interval is provided")
      "return the correct number of bits" taggedAs V260 in {
        // $ -> 00100100
        // # -> 00100011
        `DOLLAR`.set("$")
        `POUND`.set("#")
        `DOLLAR`.bitCount().futureValue should be (2)
        `POUND`.bitCount().futureValue should be (3)
      }
      Given("that some interval is provided")
      "return the correct number of bits in the specified interval" taggedAs V260 in {
        `BOTH`.set("$#")
        `BOTH`.bitCount(0, 0).futureValue should be (2)
        `BOTH`.bitCount(1, 1).futureValue should be (3)
        `BOTH`.bitCount(0, 1).futureValue should be (5)
        `BOTH`.bitCount(-6, 42).futureValue should be (5)
      }
    }
  }
  
  BitPos.toString when {
    "the key does not exists" should {
      "return -1 for set bits and 0 for clear bits" taggedAs V287 in {
        `NON-EXISTENT-KEY`.bitPos(bit = true).futureValue should be (-1)
        `NON-EXISTENT-KEY`.bitPos(bit = false).futureValue should be (0)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs V287 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.bitCount().!
        }
      }
    }
    "the key contains $" should {
      "return the position of the first bit set/unset" taggedAs V287 in {
        val bytes = new Array[Byte](3)
        bytes(0) = 0xFF.toByte
        bytes(1) = 0xF0.toByte
        bytes(2) = 0x00.toByte
        val `STRING-AS-BYTES` = new SimpleKeyImpl[String, String, Array[Byte]](keyPrefix, "STRING")
        `STRING-AS-BYTES`.set(bytes)
        `STRING`.bitPos(bit = false).futureValue should be (12)
        `STRING`.bitPos(bit = true).futureValue should be (0)
        
        `STRING`.bitPos(bit = false, 1).futureValue should be (12)
        `STRING`.bitPos(bit = true, 2).futureValue should be (-1)
        `STRING`.bitPos(bit = true, 0, 1).futureValue should be (0)
        `STRING`.bitPos(bit = false, 3).futureValue should be (-1)
        `STRING`.bitPos(bit = false, 0, 0).futureValue should be (-1)
        
        `STRING`.del()
      }
    }
  }

  Decr.toString when {
    "the value is not a string" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.decr.!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs V100 in {
        `DECR`.set("hello")
        a [RedisErrorResponseException] should be thrownBy {
          `DECR`.decr.!
        }
        `DECR`.del()
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs V100 in {
        `DECR`.decr.futureValue should be (-1)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs V100 in {
        `DECR`.decr.futureValue should be (-2)
      }
    }
  }

  DecrBy.toString when {
    "the value is not a string" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.decrBy(1).!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs V100 in {
        `DECR`.set("hello")
        a [RedisErrorResponseException] should be thrownBy {
          `DECR`.decrBy(5).!
        }
        `DECR`.del()
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs V100 in {
        `DECR`.decrBy(5).futureValue should be (-5)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs V100 in {
        `DECR`.decrBy(5).futureValue should be (-10)
      }
    }
  }

  Get.toString when {
    "the key does not exist" should {
      "return None" taggedAs V100 in {
        `NON-EXISTENT-KEY`.get.futureValue should be (empty)
      }
    }
    "the key exists but sotred value is not a string" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.get.!
        }
      }
    }
    "the key contains a string" should {
      "return the string" taggedAs V100 in {
        `STR`.set("VALUE")
        `STR`.get.futureValue should contain ("VALUE")
      }
    }
  }

  GetBit.toString when {
    "the key does not exist" should {
      "return false" taggedAs V220 in {
        `NON-EXISTENT-KEY`.getBit(0).futureValue should be (BIT_NOT_SET)
      }
    }
    "the key exists but the value is not a string" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.getBit(0).!
        }
      }
    }
    "the key exists but the offset is out of bound" should {
      "return an error" taggedAs V220 in {
        `DOLLAR`.set("$")
        a [RedisErrorResponseException] should be thrownBy {
          `DOLLAR`.getBit(-1).!
        }
      }
    }
    "the key exists" should {
      "return the correct values" taggedAs V220 in {
        // $ -> 00100100
        `DOLLAR`.getBit(0).futureValue should be (BIT_NOT_SET)
        `DOLLAR`.getBit(1).futureValue should be (BIT_NOT_SET)
        `DOLLAR`.getBit(2).futureValue should be (BIT_SET)
        `DOLLAR`.getBit(3).futureValue should be (BIT_NOT_SET)
        `DOLLAR`.getBit(4).futureValue should be (BIT_NOT_SET)
        `DOLLAR`.getBit(5).futureValue should be (BIT_SET)
        `DOLLAR`.getBit(6).futureValue should be (BIT_NOT_SET)
        `DOLLAR`.getBit(7).futureValue should be (BIT_NOT_SET)

        `DOLLAR`.getBit(8).futureValue should be (BIT_NOT_SET)
        `DOLLAR`.getBit(9).futureValue should be (BIT_NOT_SET)
      }
    }
  }
  
  GetRange.toString when {
    "the key does not exist" should {
      "return the empty string" taggedAs V240 in {
        `NON-EXISTENT-KEY`.getRange(0, 0).futureValue should be (empty)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs V240 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.getRange(0, 0).!
        }
      }
    }
    "the key exists and contains an empty string" should {
      "return the empty string" taggedAs V240 in {
        `STR`.set("")
        `STR`.getRange(0, 100).futureValue should be (empty)
      }
    }
    "the key exists and contains a non-empty string" should {
      "return the correct substring" taggedAs V240 in {
        `STR`.set("01234")
        `STR`.getRange(0, 100).futureValue should be ("01234")
        `STR`.getRange(0, 4).futureValue should be ("01234")
        `STR`.getRange(0, 2).futureValue should be ("012")
        `STR`.getRange(0, 0).futureValue should be ("0")
        `STR`.getRange(4, 4).futureValue should be ("4")
        `STR`.getRange(0, -1).futureValue should be ("01234")
        `STR`.getRange(-3, -1).futureValue should be ("234")
        `STR`.getRange(-1, -2).futureValue should be (empty)
      }
    }
  }

  GetSet.toString when {
    "the key does not exist" should {
      "return None and set the value" taggedAs V100 in {
        `STR`.del()
        `STR`.getSet(SomeValue).futureValue should be (empty)
        `STR`.get.futureValue should contain (SomeValue)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.getSet("A").!
        }
      }
    }
    "the key exists and has a string value" should {
      "return the current value and set the new value" taggedAs V100 in {
        `STR`.getSet("YO").futureValue should contain (SomeValue)
        `STR`.get.futureValue should contain ("YO")
      }
    }
  }

  Incr.toString when {
    "the value is not a string" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.incr.!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs V100 in {
        `INCR`.set("hello")
        a [RedisErrorResponseException] should be thrownBy {
          `INCR`.incr.!
        }
        `INCR`.del()
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs V100 in {
        `INCR`.incr.futureValue should be (1)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs V100 in {
        `INCR`.incr.futureValue should be (2)
      }
    }
  }

  IncrBy.toString when {
    "the value is not a string" should {
      "return an error" taggedAs V100 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.incrBy(1).!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs V100 in {
        `INCR`.set("hello")
        a [RedisErrorResponseException] should be thrownBy {
          `INCR`.incrBy(5).!
        }
        `INCR`.del()
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs V100 in {
        `INCR`.incrBy(5).futureValue should be (5)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs V100 in {
        `INCR`.incrBy(5).futureValue should be (10)
      }
    }
  }

  IncrByFloat.toString when {
    "the value is not a string" should {
      "return an error" taggedAs V260 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.incrByFloat(1.0).!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs V260 in {
        `INCR`.set("hello")
        a [RedisErrorResponseException] should be thrownBy {
          `INCR`.incrByFloat(1.2).!
        }
        `INCR`.del()
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs V260 in {
        `INCR`.incrByFloat(1.2).futureValue should be (1.2)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs V260 in {
        `INCR`.incrByFloat(2.8).futureValue should be (4)
        `INCR`.incrByFloat(120e-2).futureValue should be (5.2)
      }
    }
  }
  
  PSetEX.toString when {
    "the key does not exist" should {
      "succeed" taggedAs V260 in {
        `TO-EXPIRE`.pSetEX(SomeValue, 500)
        `TO-EXPIRE`.get.futureValue should contain (SomeValue)
        Thread.sleep(550)
        `TO-EXPIRE`.exists.futureValue should be (NOT_EXISTS)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs V260 in {
        `LIST`.pSetEX(SomeValue, 500)
        `LIST`.get.futureValue should contain (SomeValue)
        Thread.sleep(550)
        `LIST`.exists.futureValue should be (NOT_EXISTS)
        client.lPush(keyPrefix + "LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs V260 in {
        `TO-EXPIRE`.set(SomeValue)
        `TO-EXPIRE`.pSetEX("Hello", 500)
        `TO-EXPIRE`.get.futureValue should contain ("Hello")
        Thread.sleep(550)
        `TO-EXPIRE`.exists.futureValue should be (NOT_EXISTS)
      }
    }
  }

  Set.toString when {
    "setting a key that do not exist" should {
      "succeed" taggedAs V100 in {
        `NON-EXISTENT-KEY`.set(SomeValue)
        `NON-EXISTENT-KEY`.get.futureValue should contain (SomeValue)
      }
    }
    "setting a key that already exists" should {
      "succeed" taggedAs V100 in {
        `NON-EXISTENT-KEY`.set("A")
        `NON-EXISTENT-KEY`.get.futureValue should contain ("A")
        `NON-EXISTENT-KEY`.del()
      }
    }
    "setting a key of another type" should {
      "succeed" taggedAs V100 in {
        `LIST`.set(SomeValue)
        `LIST`.get.futureValue should contain (SomeValue)
        `LIST`.del()
        client.lPush(keyPrefix + "LIST", "A")
      }
    }
    "setWithOptions" should {
      `KEY`.del()
      
      When("expireAfter is provided")
      Given("the target key does not exist")
      "successfully set the value at key and expire it" taggedAs V2612 in {
        `KEY`.set(SomeValue, ttlOpt = Some(500.milliseconds)).futureValue should be (SET_CORRECTLY)
        `KEY`.get.futureValue should contain (SomeValue)
        Thread.sleep(550)
        `KEY`.get.futureValue should be (empty)
      }
      
      When("NX condition is provided")
      Given("the target key does not exist")
      "successfully set the value at key." taggedAs V2612 in {
        `KEY`.set("A", conditionOpt = Some(Condition.NX)).futureValue should be (SET_CORRECTLY)
        `KEY`.get.futureValue should contain ("A")
      }
      Given("the target key already exists")
      "return false" taggedAs V2612 in {
        `KEY`.set("B", conditionOpt = Some(Condition.NX)).futureValue should be (CONDITION_NOT_MET)
        `KEY`.get.futureValue should contain ("A")
      }
      
      When("XX condition is provided")
      Given("the target key already exists")
      "successfully set the value at key.." taggedAs V2612 in {
        `KEY`.set("B", conditionOpt = Some(Condition.XX)).futureValue should be (SET_CORRECTLY)
        `KEY`.get.futureValue should contain ("B")
      }
      Given("the target key does not exist")
      "return false." taggedAs V2612 in {
        `KEY`.del().futureValue
        `KEY`.set("C", conditionOpt = Some(Condition.XX)).futureValue should be (CONDITION_NOT_MET)
        `KEY`.get.futureValue should be (empty)
      }
      
      When("both expireAfter and a condition are provided")
      "succeed" taggedAs V2612 in {
        `KEY`.set("C", ttlOpt = Some(500.milliseconds), conditionOpt = Some(Condition.NX)).futureValue should be (SET_CORRECTLY)
        `KEY`.get.futureValue should contain ("C")
        Thread.sleep(550)
        `KEY`.get.futureValue should be (empty)
      }
    }
  }

  SetBit.toString when {
    "the key does not exist" should {
      "succeed" taggedAs V220 in {
        `BIT`.setBit(0, bit = true)
        `BIT`.setBit(3, bit = true)
        `BIT`.getBit(0).futureValue should be (BIT_SET)
        `BIT`.getBit(1).futureValue should be (BIT_NOT_SET)
        `BIT`.getBit(2).futureValue should be (BIT_NOT_SET)
        `BIT`.getBit(3).futureValue should be (BIT_SET)
        `BIT`.bitCount().futureValue should be (2)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.setBit(0, bit = true).!
        }
      }
    }
    "the key exists" should {
      "succeed" taggedAs V220 in {
        `BIT`.setBit(0, bit = false)
        `BIT`.setBit(1, bit = true)
        `BIT`.setBit(2, bit = true)
        `BIT`.getBit(0).futureValue should be (BIT_NOT_SET)
        `BIT`.getBit(1).futureValue should be (BIT_SET)
        `BIT`.getBit(2).futureValue should be (BIT_SET)
        `BIT`.getBit(3).futureValue should be (BIT_SET)
        `BIT`.bitCount().futureValue should be (3)
      }
    }
  }

  SetEX.toString when {
    "the key does not exist" should {
      "succeed" taggedAs V200 in {
        `TO-EXPIRE`.setEX(SomeValue, 1)
        `TO-EXPIRE`.get.futureValue should contain (SomeValue)
        Thread.sleep(1050)
        `TO-EXPIRE`.exists.futureValue should be (NOT_EXISTS)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs V200 in {
        `LIST`.setEX(SomeValue, 1)
        `LIST`.get.futureValue should contain (SomeValue)
        Thread.sleep(1050)
        `LIST`.exists.futureValue should be (NOT_EXISTS)
        client.lPush(keyPrefix + "LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs V200 in {
        `TO-EXPIRE`.set(SomeValue)
        `TO-EXPIRE`.setEX("Hello", 1)
        `TO-EXPIRE`.get.futureValue should contain ("Hello")
        Thread.sleep(1050)
        `TO-EXPIRE`.exists.futureValue should be (NOT_EXISTS)
      }
    }
  }

  SetNX.toString when {
    "the key does not exist" should {
      "succeed" taggedAs V100 in {
        `NON-EXISTENT-KEY`.setNX(SomeValue).futureValue should be (SET_CORRECTLY)
        `NON-EXISTENT-KEY`.get.futureValue should contain (SomeValue)
        `NON-EXISTENT-KEY`.del()
      }
    }
    "the key already exists" should {
      "do nothing" taggedAs V100 in {
        `I-EXIST`.set("YEP")
        `I-EXIST`.setNX(SomeValue).futureValue should be (NOT_EXISTS)
        `I-EXIST`.get.futureValue should contain ("YEP")
        `LIST`.setNX("YEP").futureValue should be (NOT_EXISTS)
      }
    }
  }

  SetRange.toString when {
    "the key does not exist" should {
      "succeed" taggedAs V220 in {
        `STR`.del()
        `STR`.setRange(5, "YES")
        `STR`.exists.futureValue should be (EXISTS)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.setRange(5, "YES").!
        }
      }
    }
    "the key already exists" should {
      "succeed" taggedAs V220 in {
        `STR`.setRange(0, "HELLO")
        `STR`.get.futureValue should contain ("HELLOYES")
      }
    }
  }

  StrLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs V220 in {
        `STR`.del()
        `STR`.strLen.futureValue should be (0)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs V220 in {
        a [RedisErrorResponseException] should be thrownBy {
          `LIST`.strLen.!
        }
      }
    }
    "the key contains a string" should {
      "succeed and return the correct length" taggedAs V220 in {
        `STR`.set("")
        `STR`.strLen.futureValue should be (0)
        `STR`.set("Hello")
        `STR`.strLen.futureValue should be (5)
      }
    }
  }

  override def afterAll(configMap: ConfigMap) {
    client.flushAll().!
    client.quit().!
  } 

}