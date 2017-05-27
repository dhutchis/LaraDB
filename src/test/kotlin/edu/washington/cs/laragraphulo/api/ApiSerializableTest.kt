package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.opt.AccumuloConfigImpl
import edu.washington.cs.laragraphulo.opt.SerializationUtil
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.Serializable

/** Test serializability of things. */
class ApiSerializableTest {

  /**
   * Serialize and deserialize the [obj].
   * @return The new object deserialized
   */
  private fun <S : Serializable> testSerialize(obj: S): S {
    val serialized = SerializationUtil.serialize(obj)
    val deserialized = SerializationUtil.deserialize(serialized)
    assertNotSame(obj, deserialized)
    assertEquals(obj.javaClass, deserialized.javaClass)
    @Suppress("UNCHECKED_CAST")
    (deserialized as S)
    assertEquals(obj, deserialized)

    // base64 string
    val ser = SerializationUtil.serializeBase64(obj)
    val des = SerializationUtil.deserializeBase64(ser)
    assertNotSame(obj, des)
    assertEquals(obj.javaClass, des.javaClass)
    @Suppress("UNCHECKED_CAST")
    (des as S)
    assertEquals(obj, des)

    return deserialized
  }

  @Test
  fun testSpecialSerializable() {
    val ac = AccumuloConfigImpl("instance", "localhost:2181", "root", PasswordToken("secret"))
    val newac = testSerialize(ac)
    // make sure the final fields set correctly
    assertEquals(ac.authenticationToken, newac.authenticationToken)
    assertEquals(ac.connected, newac.connected)

    val tf = TimesFun("Max",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,LType.DOUBLE,Math::max)
    val tf2 = testSerialize(tf)
    assertEquals(tf.resultZero, tf2.resultZero)
  }

  companion object {
    @JvmStatic @Suppress("UNUSED")
    fun testCases(): List<Serializable> = listOf(SensorQuery.attrT, SensorQuery.binFun, SensorQuery.createCntFun,
        SensorQuery.anyFun, SensorQuery.divideMinusOneFun, SensorQuery.initialSchema,
        SensorQuery.initialSchema.defaultPSchema(), SensorQuery.C)
    // SensorQuery.binFun.extFun as Serializable
    // ^^^ functions are serializable but cannot be compared for equality
  }

  @ParameterizedTest
  @MethodSource(names = arrayOf("testCases"))
  fun testSchema(obj: Serializable) {
    testSerialize(obj)
  }

}
