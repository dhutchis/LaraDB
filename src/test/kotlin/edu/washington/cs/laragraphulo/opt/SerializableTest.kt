package edu.washington.cs.laragraphulo.opt

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.junit.Assert.*
import org.junit.Test
import java.io.Serializable

/** Test serializability of things. */
class SerializableTest {

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
    return deserialized
  }

  @Test
  fun testAccumuloConfig() {
    val ac = AccumuloConfigImpl("instance", "localhost:2181", "root", PasswordToken("secret"))
    val newac = testSerialize(ac)
    // make sure the final fields set correctly
    assertEquals(ac.authenticationToken, newac.authenticationToken)
    assertEquals(ac.connected, newac.connected)
  }

}
