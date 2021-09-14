package edu.washington.cs.laragraphulo.encoding


import com.google.common.primitives.Ints
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder
import org.apache.accumulo.core.util.ByteBufferUtil
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparator
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.nio.ByteBuffer
import java.util.*
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import kotlin.experimental.and

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
            val c1 = WritableComparator.compareBytes(ba1,0,2,ba2,0,2)
            val es1 = ByteBufferUtil.toBytes(escape(ba1))
            val es2 = ByteBufferUtil.toBytes(escape(ba2))
            val c2 = WritableComparator.compareBytes(es1,0,es1.size,es2,0,es2.size)
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
  @Ignore("Ignored because this test is slow. The innermost body executes 5^10 = 9.7M times. " +
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
    assertEncodeDecode(listOf(
        byteArrayOf(5, 0, 2, 2),
        byteArrayOf(3),
        byteArrayOf(),
        byteArrayOf(7, 1)
    ).map { ByteBuffer.wrap(it) })
  }

  @Test
  fun testEncodeDecodeEmpty() {
    assertEncodeDecode(listOf(
        byteArrayOf()
    ).map { ByteBuffer.wrap(it) })
//    assertEncodeDecode(listOf()) fails
  }

  fun assertEncodeDecode(listOrig: List<ByteBuffer>) {
    val bbEnc = escapeAndJoin(listOrig)
    val listNew = splitAndUnescape(ByteBufferUtil.toBytes(bbEnc))
    assertEquals(listOrig, listNew)
  }


  @Test
  @Suppress("unused")
  fun printByteTable() {
    val lex = UIntegerLexicoder()
    for (i in (Byte.MIN_VALUE..Byte.MAX_VALUE).union((Byte.MAX_VALUE+1)..(Byte.MAX_VALUE+3))) {
      val b = i.toByte()
//      System.out.format("%+4d %3x%n", b, b)
      System.out.format("%+4d %3x : ", b, b)
      for (byte in lex.encode(i)) {
        System.out.format("%3x ", byte)
      }
      System.out.println()
    }
  }

  @Test
  @Suppress("unused")
  fun printIntegerTable() {
    val lex = UIntegerLexicoder()
    for ( i1 in (-5..270).union(((1 shl 16)-2)..((1 shl 16)+2)).union(((1 shl 24)-2)..((1 shl 24)+2)) ) {
      //      System.out.format("%+4d %3x%n", b, b)
      val i = i1 //Integer.reverseBytes(i1)
      System.out.format("%+4d %3x : ", i, i)
      for (byte in Ints.toByteArray(i)) {
        System.out.format("%3x ", byte)
      }
      System.out.println()
    }
  }

  @Test
  @Suppress("unused")
  fun printReverseByteTable() {
    for( i0 in (0..Byte.MAX_VALUE).union(Byte.MIN_VALUE..-1).union(256..260) ) {
      val brev: Byte = VariableLengthHelper.lastByteReversed(i0)
      val i = i0.toByte()
      val iadj = (if( i < 0 ) (128+(i and 0x7F)).toByte() else i)
      System.out.format("%+4d %02x : %02x%n", iadj, i.toByte(), brev)

//      if( i % 8 == 0 && i != 0)
//        System.out.println()
//      System.out.format("(byte)0x%02x, ", brev)
    }
  }

  @Test
  @Suppress("unused")
  fun testUIntLexicoder() {
    val a = 120
    val b = 130
    assertTrue(a < b)
    val lex = UIntegerLexicoder()
    val x = lex.encode(a)
    val y = lex.encode(b)
    System.out.println("x: "+ Arrays.toString(x)) // 1, 120
    System.out.println("y: "+ Arrays.toString(y)) // 1, -126
    assertTrue(WritableComparator.compareBytes(x, 0, x.size, y, 0, y.size) < 0)
    assertTrue(Text(x) < Text(y))
//    for (i in x.indices) // fails test
//      if( x[i] != y[i] )
//        assertTrue(x[i] < y[i])
  }



  /** Show that shorter strings come before longer strings. */
  @Test
  fun compareStringLengths() {
    val s1 = "abcd" // "greater" - expect return value > 0
    val s2 = "ab"   // "lesser"
    val t1 = Text(s1)
    val t2 = Text(s2)
    assertEquals(0, WritableComparator.compareBytes(t1.bytes, 0, t2.length, t2.bytes, 0, t2.length))
    assertTrue("expect abcd > ab", WritableComparator.compareBytes(t1.bytes, 0, t1.length, t2.bytes, 0, t2.length) > 0)

    val a = Text("10")  // "greater"
    val b = Text("110")
    assertTrue("expect \"10\" < \"111\" for strings", WritableComparator.compareBytes(a.bytes, 0, a.length, b.bytes, 0, b.length) < 0)

    val x = byteArrayOf(1, 0)    // "greater"
    val y = byteArrayOf(1, 1, 0)
    assertTrue("expect 1,0 < 1,1,1 for bytes", WritableComparator.compareBytes(x, 0, x.size, y, 0, y.size) < 0)
  }

  /** Determine which is greater: -1 (FF) or 0 (00). */
  @Test
  fun compareNegativeWithZero() {
    val n = byteArrayOf(-1) // 0xFF "greatest"
    val z = byteArrayOf(0)  // 0x00 "least"
    assertTrue("expect 0xFF > 0x00", WritableComparator.compareBytes(n,0,1,z,0,1) > 0)
  }


}
