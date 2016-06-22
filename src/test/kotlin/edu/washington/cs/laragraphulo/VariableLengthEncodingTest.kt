package edu.washington.cs.laragraphulo


import org.apache.accumulo.core.util.ByteBufferUtil
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparator
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class VariableLengthEncodingTest {

  /**
   * Test the following sort order:
   *
   * Let `1` be the escape character. Let `0` be the separator character.
   *
   * 1. `1|9999 -> 1109999`
   * 1. `103|99 -> 11103099`
   * 1. `11|999 -> 11110999
   * 1. `123|99 -> 1123099`
   * 1. `124|99 -> 1124099`
   * 1. `1244|9 -> 1124409`
   */
  @Test
  fun testEscapeInvertibleAndOrderMaintained() {
    val ba1 = ByteArray(2)
    val ba2 = ByteArray(2)
    for (i in -3..3) {
      ba1[0] = i.toByte()
      for (j in -3..3) {
        ba1[1] = j.toByte()
        for (a in -3..3) {
          ba2[0] = a.toByte()
          for (b in -3..3) {
            ba2[1] = b.toByte()
            val c1 = WritableComparator.compareBytes(ba1,0,2,ba2,0,2);
            val es1 = ByteBufferUtil.toBytes(escape(ba1))
            val es2 = ByteBufferUtil.toBytes(escape(ba2))
            val c2 = WritableComparator.compareBytes(es1,0,es1.size,es2,0,es2.size);
            val or1 = ByteBufferUtil.toBytes(unescape(es1))
            val or2 = ByteBufferUtil.toBytes(unescape(es2))
            Assert.assertArrayEquals(ba1, or1)
            Assert.assertArrayEquals(ba2, or2)
            assertEquals(Integer.signum(c1), Integer.signum(c2))
          }
        }
      }
    }
  }

  @Test
  fun testEncodeDecodeFull() {
    val ba = ByteArray(4)
    for (p in 0..4) {
      val p1 = ByteBuffer.wrap(ba, 0, p)
      val p2 = ByteBuffer.wrap(ba, p, 4 - p)
      val listOrig = listOf(p1, p2)
      for (i in -3..3) {
        ba[0] = i.toByte()
        for (j in -3..3) {
          ba[1] = j.toByte()
          for (a in -3..3) {
            ba[2] = a.toByte()
            for (b in -3..3) {
              ba[3] = b.toByte()

              val bb = escapeAndJoin(listOrig)
              val listNew = splitAndUnescape(ByteBufferUtil.toBytes(bb))
              assertEquals(listOrig, listNew)
            }
          }
        }
      }
    }
  }

  @Test
  fun testEncodeDecodeFullMini() {
    val ba = ByteArray(2)
    val ba2 = ByteArray(2)
    for (p1 in 0..2) {
      val p11 = ByteBuffer.wrap(ba, 0, p1)
      val p12 = ByteBuffer.wrap(ba, p1, 2 - p1)
      val listOrig = listOf(p11, p12)
      for (i in -2..2) {
        ba[0] = i.toByte()
        for (j in -2..2) {
          ba[1] = j.toByte()

          val bb = escapeAndJoin(listOrig)
          val listNew = splitAndUnescape(ByteBufferUtil.toBytes(bb))
          assertEquals(listOrig, listNew)
          val eb = ByteBufferUtil.toBytes(bb)

          for (p2 in 0..2) {
            val p21 = ByteBuffer.wrap(ba2, 0, p2)
            val p22 = ByteBuffer.wrap(ba2, p2, 2 - p2)
            val listOrig2 = listOf(p21, p22)
            for (i2 in -2..2) {
              ba2[0] = i2.toByte()
              for (j2 in -2..2) {
                ba2[1] = j2.toByte()

                val bb2 = escapeAndJoin(listOrig2)

                var co = WritableComparator.compareBytes(ba, 0, p1, ba2, 0, p2)
                if (co == 0)
                  co = WritableComparator.compareBytes(ba, p1, 2 - p1, ba2, p2, 2 - p2)

                val eb2 = ByteBufferUtil.toBytes(bb2)
                var cn = WritableComparator.compareBytes(eb, 0, eb.size, eb2, 0, eb2.size)
                if (cn == 0)
                  cn = WritableComparator.compareBytes(eb, 0, eb.size, eb2, 0, eb2.size)
                assertEquals(Integer.signum(co), Integer.signum(cn))
              }
            }
          }
        }
      }
    }

  }


  @Test
  @Ignore("This test passes but is quite slow. The innermost body executes 5^10 = 9.7M times. " +
      "See the lighter-weight mini version.")
  fun testEncodeDecodeFullFull() {
    val ba = ByteArray(4)
    val ba2 = ByteArray(4)
    for (p1 in 0..4) {
      val p11 = ByteBuffer.wrap(ba, 0, p1)
      val p12 = ByteBuffer.wrap(ba, p1, 4 - p1)
      val listOrig = listOf(p11, p12)
      for (i in -2..2) {
        ba[0] = i.toByte()
        for (j in -2..2) {
          ba[1] = j.toByte()
          for (a in -2..2) {
            ba[2] = a.toByte()
            for (b in -2..2) {
              ba[3] = b.toByte()

              val bb = escapeAndJoin(listOrig)
              val listNew = splitAndUnescape(ByteBufferUtil.toBytes(bb))
              assertEquals(listOrig, listNew)
              val eb = ByteBufferUtil.toBytes(bb)

              for (p2 in 0..4) {
                val p21 = ByteBuffer.wrap(ba2, 0, p2)
                val p22 = ByteBuffer.wrap(ba2, p2, 4 - p2)
                val listOrig2 = listOf(p21, p22)
                for (i2 in -2..2) {
                  ba2[0] = i2.toByte()
                  for (j2 in -2..2) {
                    ba2[1] = j2.toByte()
                    for (a2 in -2..2) {
                      ba2[2] = a2.toByte()
                      for (b2 in -2..2) {
                        ba2[3] = b2.toByte()

                        val bb2 = escapeAndJoin(listOrig2)

                        var co = WritableComparator.compareBytes(ba, 0, p1, ba2, 0, p2)
                        if (co == 0)
                          co = WritableComparator.compareBytes(ba, p1, 4 - p1, ba2, p2, 4 - p2)

                        val eb2 = ByteBufferUtil.toBytes(bb2)
                        var cn = WritableComparator.compareBytes(eb, 0, eb.size, eb2, 0, eb2.size)
                        if (cn == 0)
                          cn = WritableComparator.compareBytes(eb, 0, eb.size, eb2, 0, eb2.size)
                        assertEquals(Integer.signum(co), Integer.signum(cn))
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Assuming 0 is the separator and 1 is the escape,
   * `[ 5022, 3, '', 71 ] -> 510220300711`.
   */
  @Test
  fun testEncodeDecodeExample() {
    val listOrig = listOf(
        byteArrayOf(5, 0, 2, 2),
        byteArrayOf(3),
        byteArrayOf(),
        byteArrayOf(7, 1)
    ).map { ByteBuffer.wrap(it) }
    val bbEnc = escapeAndJoin(listOrig)
    val listNew = splitAndUnescape(ByteBufferUtil.toBytes(bbEnc))
    assertEquals(listOrig, listNew)
//    listOrig.zip(listNew).forEach { Assert.assertEquals(it.first, it.second) }
  }

  @Test
  @Ignore("Not a test.")
  fun printByteTable() {
    for (i in Byte.MIN_VALUE..Byte.MAX_VALUE) {
      val b = i.toByte()
      System.out.format("%+4d %3x%n", b, b)
    }
  }



  /** Show that shorter strings come before longer strings. */
  @Test
  fun compareStringLengths() {
    val s1 = "abcd" // "greater" - expect return value > 0
    val s2 = "ab"   // "lessor"
    val t1 = Text(s1)
    val t2 = Text(s2)
    assertEquals(0, WritableComparator.compareBytes(t1.bytes, 0, t2.length, t2.bytes, 0, t2.length))
    assertTrue(WritableComparator.compareBytes(t1.bytes, 0, t1.length, t2.bytes, 0, t2.length) > 0, "expect abcd > ab")
  }

  /** Determine which is greater: -1 (FF) or 0 (00). */
  @Test
  fun compareNegativeWithZero() {
    val n = byteArrayOf(-1) // 0xFF "greatest"
    val z = byteArrayOf(0)  // 0x00 "least"
    assertTrue(WritableComparator.compareBytes(n,0,1,z,0,1) > 0, "expect 0xFF > 0x00")
  }


}

