package edu.washington.cs.laragraphulo.api

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import edu.washington.cs.laragraphulo.LexicoderPlus
import edu.washington.cs.laragraphulo.opt.ABS
import edu.washington.cs.laragraphulo.opt.EMPTY
import org.apache.accumulo.core.client.lexicoder.*

typealias NOPE = UnsupportedOperationException


sealed class Type<T> : LexicoderPlus<T> {
  // encode: T -> BA
  // decode: BA -> T
  /** T -> S */
  fun encodeString(t: T): String = t.toString()
  /** S -> T */
  abstract fun decodeString(s: String): T
  /** T -> L */
  abstract fun encodeLong(t: T): Long
  /** L -> T */
  abstract fun decodeLong(l: Long): T
  abstract val naturalWidth: Int
  abstract val naturalDefault: T

  val naturalDefaultEncoded: ByteArray by lazy { encode(naturalDefault) }
  override abstract fun toString(): String

  /*
  It is necessary to override equals()/hashCode(), even though every Type<> is a singleton object,
  because the objects are serialized and transmitted to a foreign JVM.
  In the foreign JVM the de-serialized objects are not necessarily identically equal to the singleton static objects.
  equals() and hashCode() force them to.
   */
  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Type<*>

    return true
  }

  override fun hashCode(): Int{
    return javaClass.hashCode()
  }


  /**  */
  object UNKNOWN : Type<ABS>() {
    override fun encode(v: ABS): ByteArray = v.toArray()
    override fun decode(b: ByteArray, off: Int, len: Int) = ABS(b,off,len)
    override fun decodeString(s: String) = ABS(s.toByteArray())
    override val naturalWidth = 4
    override val naturalDefault: ABS = EMPTY
    override fun toString() = "UNKNOWN"
    override fun encodeLong(t: ABS): Long = throw NOPE("no conversion from ${toString()} to Long")
    override fun decodeLong(l: Long): ABS = throw NOPE("no conversion from Long to ${toString()}")
  }

  /** Fixed width int encoding.
   * This might not preserve the order of unsigned integers.
   * */
  object INT : Type<Int>() {
    override fun encode(v: Int): ByteArray = Ints.toByteArray(v)
    override fun decode(b: ByteArray, off: Int, len: Int): Int {
      require(len == 4) {"Expected length of 4 but given $len; from offset $off bytes are $b"}
      return Ints.fromBytes(b[off],b[off+1],b[off+2],b[off+3])
    }
    override fun decodeString(s: String): Int = s.toInt()
    override val naturalWidth = 4
    override val naturalDefault: Int = 0
    override fun toString() = "INT"
    override fun encodeLong(t: Int): Long = t.toLong()
    override fun decodeLong(l: Long): Int = l.toInt()
  }
  /** See [IntegerLexicoder]. The first byte appears to store length information: between 1 and 5 bytes. */
  object INT_VARIABLE : Type<Int>() {
    val lex = IntegerLexicoder()
    override val naturalWidth = -1
    override fun encode(v: Int): ByteArray = lex.encode(v)
    override fun decode(b: ByteArray, off: Int, len: Int): Int = lex.decode(b, off, len)
    override fun decodeString(s: String) = s.toInt()
    override val naturalDefault: Int = 0
    override fun toString() = "INT_VARIABLE"
    override fun encodeLong(t: Int): Long = t.toLong()
    override fun decodeLong(l: Long): Int = l.toInt()
  }
  object LONG : Type<Long>() {
    override fun decode(b: ByteArray, off: Int, len: Int): Long = Longs.fromBytes(b[off],b[off+1],b[off+2],b[off+3],b[off+4],b[off+5],b[off+6],b[off+7])
    override fun encode(v: Long): ByteArray = Longs.toByteArray(v)
    override fun decodeString(s: String) = s.toLong()
    override val naturalWidth: Int = 8
    override val naturalDefault = 0L
    override fun toString() = "LONG"
    override fun encodeLong(t: Long): Long = t
    override fun decodeLong(l: Long): Long = l
  }
  /** See [LongLexicoder]. The first byte appears to store length information: between 1 and 9 bytes. */
  object LONG_VARIABLE : Type<Long>() {
    val lex = LongLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): Long = lex.decode(b, off, len)
    override fun encode(v: Long): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s.toLong()
    override val naturalWidth: Int = -1
    override val naturalDefault = 0L
    override fun toString() = "LONG_VARIABLE"
    override fun encodeLong(t: Long): Long = t
    override fun decodeLong(l: Long): Long = l
  }
  object BOOLEAN : Type<Boolean>() {
    const val ZERO: Byte = 0
    override fun decode(b: ByteArray, off: Int, len: Int): Boolean = b[off] != ZERO
    val FALSE = byteArrayOf(0)
    val TRUE = byteArrayOf(1)
    override fun encode(v: Boolean): ByteArray = if (v) TRUE else FALSE
    override fun decodeString(s: String) = s.toBoolean()
    override val naturalWidth: Int = 1
    override val naturalDefault = false
    override fun toString() = "BOOLEAN"
    override fun encodeLong(t: Boolean): Long = if (t) 1 else 0
    override fun decodeLong(l: Long): Boolean = l != 0L
  }
  /** Encode in terms of long bits. Probably does not preserve order. */
  object DOUBLE : Type<Double>() {
    override fun decode(b: ByteArray, off: Int, len: Int): Double = java.lang.Double.longBitsToDouble(LONG.decode(b, off, len))
    override fun encode(v: Double): ByteArray = LONG.encode(java.lang.Double.doubleToLongBits(v))
    override fun decodeString(s: String) = s.toDouble()
    override val naturalWidth: Int = 8
    override val naturalDefault = 0.0
    override fun toString() = "DOUBLE"
    override fun encodeLong(t: Double): Long = t.toLong()
    override fun decodeLong(l: Long): Double = l.toDouble()
  }
  /** See [DoubleLexicoder]. The first byte appears to store length information: between 1 and 9 bytes. */
  object DOUBLE_VARIABLE : Type<Double>() {
    val lex = DoubleLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): Double = lex.decode(b, off, len)
    override fun encode(v: Double): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s.toDouble()
    override val naturalWidth: Int = -1
    override val naturalDefault = 0.0
    override fun toString() = "DOUBLE_VARIABLE"
    override fun encodeLong(t: Double): Long = t.toLong()
    override fun decodeLong(l: Long): Double = l.toDouble()
  }
  /** UTF8 string encoding */
  object STRING : Type<String>() {
    val lex = StringLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): String = lex.decode(b, off, len)
    override fun encode(v: String): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s
    override val naturalWidth: Int = -1
    override val naturalDefault = ""
    override fun toString() = "STRING"
    override fun encodeLong(t: String): Long = t.toLong()
    override fun decodeLong(l: Long): String = l.toString()
  }
//  object DATETIME : Type<DateTime>() {
//    val lex = DateTimeLexicoder()
//    override fun decode(b: ByteArray, offset: Int, len: Int): DateTime = lex.decode(b, offset, len)
//    override fun encode(v: DateTime?): ByteArray = lex.encode(v)
//  }
  /** 4 byte constant width. Probably does not preserve order */
  object FLOAT : Type<Float>() {
    override fun decode(b: ByteArray, off: Int, len: Int): Float = java.lang.Float.intBitsToFloat(INT.decode(b, off, len))
    override fun encode(v: Float): ByteArray = INT.encode(java.lang.Float.floatToIntBits(v))
    override fun decodeString(s: String) = s.toFloat()
    override val naturalWidth: Int = 4
    override val naturalDefault = 0.0f
    override fun toString() = "FLOAT"
    override fun encodeLong(t: Float): Long = t.toLong()
    override fun decodeLong(l: Long): Float = l.toFloat()
  }
  /** See [FloatLexicoder]. The first byte appears to store length information: between 1 and 5 bytes. */
  object FLOAT_VARIABLE : Type<Float>() {
    val lex = FloatLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): Float = lex.decode(b, off, len)
    override fun encode(v: Float): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s.toFloat()
    override val naturalWidth: Int = -1
    override val naturalDefault = 0.0f
    override fun toString() = "FLOAT_VARIABLE"
    override fun encodeLong(t: Float): Long = t.toLong()
    override fun decodeLong(l: Long): Float = l.toFloat()
  }


//  object BYTE_ARRAY : Type<ByteArray>() {
//
//  }
}

