package edu.washington.cs.laragraphulo.api

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import edu.washington.cs.laragraphulo.LexicoderPlus
import edu.washington.cs.laragraphulo.opt.ABS
import edu.washington.cs.laragraphulo.opt.EMPTY
import edu.washington.cs.laragraphulo.sensor.dateParserNoTime
import org.apache.accumulo.core.client.lexicoder.*

typealias NOPE = UnsupportedOperationException



/** Logical Type. Used for specifying UDFs.
 * A default physical type that implements this logical type is [defaultPhysical]. */
sealed class LType<T> : Comparator<T> {
  /** Example values of this type. */
  abstract val examples: Set<T>
  /** What values of T are valid for this type?
   * For example, a timestamp type cannot have negative long values. */
  open fun valid(t: T): Boolean = true
  /** Default physical type implementing this logical type */
  abstract val defaultPhysical: PType<T>
  override abstract fun toString(): String

  /** ex: Measurement value, nullable */
  object NDOUBLE : LType<Double?>(), Comparator<Double?> by nullsFirst<Double>() {
    override val examples: Set<Double> = setOf(0.0, 3.1, 1.0, -2.0, -2.5, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE)
    override val defaultPhysical = PType.DOUBLE.nullable
    override fun toString() = "L-NDOUBLE"
  }

  /** Unsigned long, like a timestamp. */
  object ULONG : LType<Long>(), Comparator<Long> by naturalOrder<Long>() {
    override val examples: Set<Long> = setOf(0, 100, Long.MAX_VALUE)
    override fun valid(t: Long): Boolean = t >= 0
    override val defaultPhysical = PType.LONG // todo: add a ULONG physical type. Same with UINT below.
    override fun toString() = "L-ULONG"
  }

  /** ex: Measurement class */
  object STRING : LType<String>(), Comparator<String> by naturalOrder<String>() {
    override val examples: Set<String> = setOf("", "temperature", "humidity")
    override val defaultPhysical = PType.STRING
    override fun toString() = "L-STRING"
  }

  /** ex: Measurement value, always present */
  object DOUBLE : LType<Double>(), Comparator<Double> by naturalOrder<Double>() {
    override val examples: Set<Double> = setOf(0.0, 3.1, 1.0, -2.0, -2.5, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE)
    override val defaultPhysical = PType.DOUBLE
    override fun toString() = "L-DOUBLE"
  }

  /** ex: Count */
  object UINT : LType<Int>(), Comparator<Int> by naturalOrder<Int>() {
    override val examples: Set<Int> = setOf(0, 1, 2, 20, Int.MAX_VALUE)
    override fun valid(t: Int): Boolean = t >= 0
    override val defaultPhysical = PType.INT
    override fun toString() = "L-UINT"
  }


  /*
    It is necessary to override equals()/hashCode(), even though every PType<> is a singleton object,
    because the objects are serialized and transmitted to a foreign JVM.
    In the foreign JVM the de-serialized objects are not necessarily identically equal to the singleton static objects.
    equals() and hashCode() force them to.
     */
  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as LType<*>

    return true
  }

  override fun hashCode(): Int{
    return javaClass.hashCode()
  }
}

//fun <T,P:T> lower(t: LType<T>): PType<T> {
//  return when(this) {
//    LType.ULONG -> PType.LONG as PType<T>
//    else -> error("")
//  }
//}


//typealias LType<T> = LType0<T,T>

/** Physical Types. Use [nullable] to get nullable physical types. */
sealed class PType<T> : LexicoderPlus<T>, LType<T>() {
  // encode: T -> BA
  // decode: BA -> T
  /** T -> S */
  fun encodeString(t: T): String = t.toString()
  /** S -> T */
  abstract fun decodeString(s: String): T
  /** T -> L */
  abstract fun encodeLong(t: T): Long
  fun encodeLongUnchecked(t: Any?) = @Suppress("UNCHECKED_CAST") encodeLong(t as T)
  /** L -> T */
  abstract fun decodeLong(l: Long): T
  abstract val naturalWidth: Int
  abstract val naturalDefault: T
  val naturalDefaultEncoded: ByteArray by lazy { encode(naturalDefault) }

  abstract val nullable: PType<T?>

  private class NPType<T : Any>(
      val p: PType<T>
  ) : PType<T?>(), Comparator<T?> by nullsFirst(p) {
    override fun decode(b: ByteArray, off: Int, len: Int) = p.decode(b, off, len)
    override val examples = p.examples
    override fun encode(v: T?): ByteArray {
      require(v != null) {"not encoding null"} // use NullLexicoder to encode null, if necessary
      return p.encode(v!!)
    }
    override val defaultPhysical = this
    override fun decodeString(s: String) = p.decodeString(s)
    override fun encodeLong(t: T?): Long {
      require(t != null) {"not encoding null"}
      return p.encodeLong(t!!)
    }
    override fun decodeLong(l: Long) = p.decodeLong(l)
    override val naturalWidth = p.naturalWidth // change if we encode null
    override val naturalDefault = null
    override fun toString() = "N"+p.toString()
    override val nullable: PType<T?> = this

    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (other?.javaClass != javaClass) return false
      if (!super.equals(other)) return false

      other as NPType<*>

      if (p != other.p) return false

      return true
    }
    override fun hashCode(): Int {
      var result = super.hashCode()
      result = 31 * result + p.hashCode()
      return result
    }
  }


  object UNKNOWN : PType<ABS>(), Comparator<ABS> by naturalOrder<ABS>() {
    override val examples = setOf(EMPTY)
    override fun encode(v: ABS): ByteArray = v.toArray()
    override fun decode(b: ByteArray, off: Int, len: Int) = ABS(b,off,len)
    override fun decodeString(s: String) = ABS(s.toByteArray())
    override val naturalWidth = 4
    override val naturalDefault: ABS = EMPTY
    override fun toString() = "UNKNOWN"
    override fun encodeLong(t: ABS): Long = throw NOPE("no conversion from ${toString()} to Long")
    override fun decodeLong(l: Long): ABS = throw NOPE("no conversion from Long to ${toString()}")
    override val defaultPhysical = this
    override val nullable: PType<ABS?> = NPType(this)
  }

  /** Fixed width int encoding.
   * This might not preserve the order of unsigned integers.
   * */
  object INT : PType<Int>(), Comparator<Int> by naturalOrder<Int>() {
    override val examples = setOf(0, -1, 1, Int.MIN_VALUE, Int.MAX_VALUE)
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
    override val defaultPhysical = this
    override val nullable: PType<Int?> = NPType(this)
  }
  /** See [IntegerLexicoder]. The first byte appears to store length information: between 1 and 5 bytes. */
  object INT_VARIABLE : PType<Int>(), Comparator<Int> by naturalOrder() {
    override val examples = setOf(0, -1, 1, Int.MIN_VALUE, Int.MAX_VALUE)
    val lex = IntegerLexicoder()
    override fun encode(v: Int): ByteArray = lex.encode(v)
    override fun decode(b: ByteArray, off: Int, len: Int): Int = lex.decode(b, off, len)
    override fun decodeString(s: String) = s.toInt()
    override val naturalWidth = -1
    override val naturalDefault: Int = 0
    override fun toString() = "INT_VARIABLE"
    override fun encodeLong(t: Int): Long = t.toLong()
    override fun decodeLong(l: Long): Int = l.toInt()
    override val defaultPhysical = this
    override val nullable: PType<Int?> = NPType(this)
  }
  object LONG : PType<Long>(), Comparator<Long> by naturalOrder() {
    override val examples = setOf(0, -1, 1, Long.MIN_VALUE, Long.MAX_VALUE)
    override fun decode(b: ByteArray, off: Int, len: Int): Long = Longs.fromBytes(b[off],b[off+1],b[off+2],b[off+3],b[off+4],b[off+5],b[off+6],b[off+7])
    override fun encode(v: Long): ByteArray = Longs.toByteArray(v)
    override fun decodeString(s: String) = s.toLong()
    override val naturalWidth: Int = 8
    override val naturalDefault = 0L
    override fun toString() = "LONG"
    override fun encodeLong(t: Long): Long = t
    override fun decodeLong(l: Long): Long = l
    override val defaultPhysical = this
    override val nullable: PType<Long?> = NPType(this)
  }
  /** See [LongLexicoder]. The first byte appears to store length information: between 1 and 9 bytes. */
  object LONG_VARIABLE : PType<Long>(), Comparator<Long> by naturalOrder() {
    override val examples = setOf(0, -1, 1, Long.MIN_VALUE, Long.MAX_VALUE)
    val lex = LongLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): Long = lex.decode(b, off, len)
    override fun encode(v: Long): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s.toLong()
    override val naturalWidth: Int = -1
    override val naturalDefault = 0L
    override fun toString() = "LONG_VARIABLE"
    override fun encodeLong(t: Long): Long = t
    override fun decodeLong(l: Long): Long = l
    override val defaultPhysical = this
    override val nullable: PType<Long?> = NPType(this)
  }
  object BOOLEAN : PType<Boolean>(), Comparator<Boolean> by naturalOrder() {
    override val examples = setOf(true, false)
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
    override val defaultPhysical = this
    override val nullable: PType<Boolean?> = NPType(this)
  }
  /** Encode in terms of long bits. Probably does not preserve order. */
  object DOUBLE : PType<Double>(), Comparator<Double> by naturalOrder() {
    override val examples = setOf(0.0, 3.1, 1.0, -2.0, -2.5, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE)
    override fun decode(b: ByteArray, off: Int, len: Int): Double = java.lang.Double.longBitsToDouble(LONG.decode(b, off, len))
    override fun encode(v: Double): ByteArray = LONG.encode(java.lang.Double.doubleToLongBits(v))
    override fun decodeString(s: String) = s.toDouble()
    override val naturalWidth: Int = 8
    override val naturalDefault = 0.0
    override fun toString() = "DOUBLE"
    override fun encodeLong(t: Double): Long = t.toLong()
    override fun decodeLong(l: Long): Double = l.toDouble()
    override val defaultPhysical = this
    override val nullable: PType<Double?> = NPType(this)
  }
  /** See [DoubleLexicoder]. The first byte appears to store length information: between 1 and 9 bytes. */
  object DOUBLE_VARIABLE : PType<Double>(), Comparator<Double> by naturalOrder() {
    override val examples = setOf(0.0, 3.1, 1.0, -2.0, -2.5, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE)
    val lex = DoubleLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): Double = lex.decode(b, off, len)
    override fun encode(v: Double): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s.toDouble()
    override val naturalWidth: Int = -1
    override val naturalDefault = 0.0
    override fun toString() = "DOUBLE_VARIABLE"
    override fun encodeLong(t: Double): Long = t.toLong()
    override fun decodeLong(l: Long): Double = l.toDouble()
    override val defaultPhysical = this
    override val nullable: PType<Double?> = NPType(this)
  }
  /** UTF8 string encoding */
  object STRING : PType<String>(), Comparator<String> by naturalOrder() {
    override val examples = setOf("", "a", "A", "abcdefg")
    val lex = StringLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): String = lex.decode(b, off, len)
    override fun encode(v: String): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s
    override val naturalWidth: Int = -1
    override val naturalDefault = ""
    override fun toString() = "STRING"
    override fun encodeLong(t: String): Long = t.toLong()
    override fun decodeLong(l: Long): String = l.toString()
    override val defaultPhysical = this
    override val nullable: PType<String?> = NPType(this)
  }
//  object DATETIME : PType<DateTime>() {
//    val lex = DateTimeLexicoder()
//    override fun decode(b: ByteArray, offset: Int, len: Int): DateTime = lex.decode(b, offset, len)
//    override fun encode(v: DateTime?): ByteArray = lex.encode(v)
//  }
  /** 4 byte constant width. Probably does not preserve order */
  object FLOAT : PType<Float>(), Comparator<Float> by naturalOrder() {
    override val examples = setOf(0.0f, 3.1f, 1.0f, -2.0f, -2.5f, Float.MAX_VALUE, -Float.MAX_VALUE, Float.MIN_VALUE, -Float.MIN_VALUE)
    override fun decode(b: ByteArray, off: Int, len: Int): Float = java.lang.Float.intBitsToFloat(INT.decode(b, off, len))
    override fun encode(v: Float): ByteArray = INT.encode(java.lang.Float.floatToIntBits(v))
    override fun decodeString(s: String) = s.toFloat()
    override val naturalWidth: Int = 4
    override val naturalDefault = 0.0f
    override fun toString() = "FLOAT"
    override fun encodeLong(t: Float): Long = t.toLong()
    override fun decodeLong(l: Long): Float = l.toFloat()
    override val defaultPhysical = this
    override val nullable: PType<Float?> = NPType(this)
  }
  /** See [FloatLexicoder]. The first byte appears to store length information: between 1 and 5 bytes. */
  object FLOAT_VARIABLE : PType<Float>(), Comparator<Float> by naturalOrder() {
    override val examples = setOf(0.0f, 3.1f, 1.0f, -2.0f, -2.5f, Float.MAX_VALUE, -Float.MAX_VALUE, Float.MIN_VALUE, -Float.MIN_VALUE)
    val lex = FloatLexicoder()
    override fun decode(b: ByteArray, off: Int, len: Int): Float = lex.decode(b, off, len)
    override fun encode(v: Float): ByteArray = lex.encode(v)
    override fun decodeString(s: String) = s.toFloat()
    override val naturalWidth: Int = -1
    override val naturalDefault = 0.0f
    override fun toString() = "FLOAT_VARIABLE"
    override fun encodeLong(t: Float): Long = t.toLong()
    override fun decodeLong(l: Long): Float = l.toFloat()
    override val defaultPhysical = this
    override val nullable: PType<Float?> = NPType(this)
  }


//  object BYTE_ARRAY : PType<ByteArray>() {
//
//  }
}

