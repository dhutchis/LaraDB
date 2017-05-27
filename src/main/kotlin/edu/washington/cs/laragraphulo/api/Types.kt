package edu.washington.cs.laragraphulo.api

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import edu.washington.cs.laragraphulo.LexicoderPlus
import edu.washington.cs.laragraphulo.opt.ABS
import edu.washington.cs.laragraphulo.opt.EMPTY
import org.apache.accumulo.core.client.lexicoder.*
import java.io.*
import java.util.*

// A mess used because delegates can't be marked transient

/** Same as [kotlin.comparisons.NaturalOrderComparator], but also Serializable. */
private object NaturalOrderComparator : Comparator<Comparable<Any>>, Serializable {
  override fun compare(a: Comparable<Any>, b: Comparable<Any>): Int = a.compareTo(b)
  @Suppress("VIRTUAL_MEMBER_HIDDEN")
  fun reversed(): Comparator<Comparable<Any>> = ReverseOrderComparator
  override fun equals(other: Any?): Boolean = this === other || javaClass == other?.javaClass
  override fun hashCode(): Int = javaClass.hashCode()
}
private object ReverseOrderComparator: Comparator<Comparable<Any>>, Serializable {
  override fun compare(a: Comparable<Any>, b: Comparable<Any>): Int = b.compareTo(a)
  @Suppress("VIRTUAL_MEMBER_HIDDEN")
  fun reversed(): Comparator<Comparable<Any>> = NaturalOrderComparator
  override fun equals(other: Any?): Boolean = this === other || javaClass == other?.javaClass
  override fun hashCode(): Int = javaClass.hashCode()
}
fun <T:Comparable<T>> serialNaturalOrder() = @Suppress("UNCHECKED_CAST") (NaturalOrderComparator as Comparator<T>)
fun <T:Comparable<T>> serialReverseOrder() = @Suppress("UNCHECKED_CAST") (NaturalOrderComparator as Comparator<T>)

// compiler bug prevents static enforcement that comparator should be Serializable
private class SerialNullsFirstComparator<T>(
    val comparator: Comparator<in T>
) : Comparator<T?>, Serializable {
  init {
    require(comparator is Serializable) {"comparator required to be serializable: $comparator"}
  }
  override fun compare(a: T?, b: T?): Int {
    if (a === b) return 0
    if (a == null) return -1
    if (b == null) return 1
    return comparator.compare(a, b)
  }
  @Suppress("VIRTUAL_MEMBER_HIDDEN")
  fun reversed(): Comparator<Comparable<Any>> = ReverseOrderComparator
  override fun equals(other: Any?): Boolean = this === other || javaClass == other?.javaClass
  override fun hashCode(): Int = javaClass.hashCode()
}
fun <T: Any> serialNullsFirst(comparator: Comparator<in T>): Comparator<T?> = SerialNullsFirstComparator(comparator)
fun <T: Comparable<T>> serialNullsFirst(): Comparator<T?> = SerialNullsFirstComparator(serialNaturalOrder<T>())




/** Logical Type. Used for specifying UDFs.
 * A default physical type that implements this logical type is [defaultPhysical]. */
sealed class LType<T> : Comparator<T>, Serializable {
  /** Example values of this type. */
  abstract val examples: Set<T>
  /** What values of T are valid for this type?
   * For example, a timestamp type cannot have negative long values. */
  open fun valid(t: T): Boolean = true
  /** Default physical type implementing this logical type */
  abstract val defaultPhysical: PType<T>
  override abstract fun toString(): String
  @Suppress("UNCHECKED_CAST")
  fun compareUnchecked(o1: Any?, o2: Any?): Int = compare(o1 as T, o2 as T)
  /** A least value, for comparison purposes */
  abstract val MIN_VALUE: T
  /** A greatest value, for comparison purposes */
  abstract val MAX_VALUE: T

  /** ex: Measurement value, nullable */
  object NDOUBLE : LType<Double?>(), Comparator<Double?> by serialNullsFirst<Double>() {
    override val examples: Set<Double> = setOf(0.0, 3.1, 1.0, -2.0, -2.5, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE)
    override val defaultPhysical = PType.DOUBLE.nullable
    override fun toString() = "L-NDOUBLE"
    override val MIN_VALUE: Double = -Double.NEGATIVE_INFINITY
    override val MAX_VALUE: Double = Double.POSITIVE_INFINITY
  }

  /** Unsigned long, like a timestamp. */
  object ULONG : LType<Long>(), Comparator<Long> by serialNaturalOrder<Long>() {
    override val examples: Set<Long> = setOf(0, 100, Long.MAX_VALUE)
    override fun valid(t: Long): Boolean = t >= 0
    override val defaultPhysical = PType.LONG // todo: add a ULONG physical type. Same with UINT below.
    override fun toString() = "L-ULONG"
    override val MIN_VALUE = 0L
    override val MAX_VALUE = Long.MAX_VALUE
  }

  /** ex: Measurement class */
  object STRING : LType<String>(), Comparator<String> by serialNaturalOrder<String>() {
    override val examples: Set<String> = setOf("", "temperature", "humidity")
    override val defaultPhysical = PType.STRING
    override fun toString() = "L-STRING"
    override val MIN_VALUE = PType.STRING.MIN_VALUE
    override val MAX_VALUE = PType.STRING.MAX_VALUE
  }

  /** ex: Measurement value, always present */
  object DOUBLE : LType<Double>(), Comparator<Double> by serialNaturalOrder<Double>() {
    override val examples: Set<Double> = setOf(0.0, 3.1, 1.0, -2.0, -2.5, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -Double.MIN_VALUE)
    override val defaultPhysical = PType.DOUBLE
    override fun toString() = "L-DOUBLE"
    override val MIN_VALUE: Double = -Double.NEGATIVE_INFINITY
    override val MAX_VALUE: Double = Double.POSITIVE_INFINITY
  }

  /** ex: Count */
  object UINT : LType<Int>(), Comparator<Int> by serialNaturalOrder<Int>() {
    override val examples: Set<Int> = setOf(0, 1, 2, 20, Int.MAX_VALUE)
    override fun valid(t: Int): Boolean = t >= 0
    override val defaultPhysical = PType.INT
    override fun toString() = "L-UINT"
    override val MIN_VALUE = 0
    override val MAX_VALUE = Integer.MAX_VALUE
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
  ) : PType<T?>(), Comparator<T?> by serialNullsFirst(p) {
    companion object {
      const val NULL_LONG = Long.MIN_VALUE+1
    }
    private val nl = NullLexicoder(p)
    override fun decode(b: ByteArray, off: Int, len: Int) = nl.decode(b, off, len)
    override val examples = p.examples
    override fun encode(v: T?): ByteArray {
//      require(v != null) {"not encoding null"} // use NullLexicoder to encode null, if necessary
      // FORCING TO FIXED WIDTH - encoding nulls is much better if we have variable width' can use a single zero
//      return if (v == null) Arrays.copyOf(nl.encode(v), naturalWidth)
//       else nl.encode(v)
      return nl.encode(v)
    }
    override val defaultPhysical = this
    override fun decodeString(s: String) = p.decodeString(s)
    override fun encodeLong(t: T?): Long = if (t == null) NULL_LONG else p.encodeLong(t).also {
      if (it == NULL_LONG) throw IllegalStateException("Bad luck: attempt to encode the sentinel long $NULL_LONG. " +
          "Looks like we need a better way to encode nulls into longs.")
    }
    override fun decodeLong(l: Long) = if (l == NULL_LONG) null else p.decodeLong(l)
    override val naturalWidth = -1
    override val naturalDefault = null
    override fun toString() = "N"+p.toString()
    override val nullable: PType<T?> = this
    override val MIN_VALUE = p.MIN_VALUE
    override val MAX_VALUE = p.MAX_VALUE

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


  object UNKNOWN : PType<ABS>(), Comparator<ABS> by serialNaturalOrder<ABS>() {
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
    override val MIN_VALUE = ABS(byteArrayOf())
    override val MAX_VALUE = ABS(byteArrayOf(Byte.MAX_VALUE,Byte.MAX_VALUE,Byte.MAX_VALUE,Byte.MAX_VALUE,Byte.MAX_VALUE))
  }

  /** Fixed width int encoding.
   * This might not preserve the order of unsigned integers.
   * */
  object INT : PType<Int>(), Comparator<Int> by serialNaturalOrder<Int>() {
    override val examples = setOf(0, -1, 1, Int.MIN_VALUE, Int.MAX_VALUE)
    override fun encode(v: Int): ByteArray = Ints.toByteArray(v)
    override fun decode(b: ByteArray, off: Int, len: Int): Int {
      require(len == 4) {"Expected length of 4 but given $len; from offset $off bytes are ${Arrays.toString(b)}"}
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
    override val MIN_VALUE = Integer.MIN_VALUE
    override val MAX_VALUE = Integer.MAX_VALUE
  }
  /** See [IntegerLexicoder]. The first byte appears to store length information: between 1 and 5 bytes. */
  object INT_VARIABLE : PType<Int>(), Comparator<Int> by serialNaturalOrder() {
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
    override val MIN_VALUE = Integer.MIN_VALUE
    override val MAX_VALUE = Integer.MAX_VALUE
  }
  object LONG : PType<Long>(), Comparator<Long> by serialNaturalOrder() {
    override val examples = setOf(0, -1, 1, Long.MIN_VALUE, Long.MAX_VALUE)
    override fun decode(b: ByteArray, off: Int, len: Int): Long {
//      println("LONG: off is $off; len is $len; ${Arrays.toString(b)}")
      require(len == 8) {"bad length. off is $off; len is $len; ${Arrays.toString(b)}"}
      return Longs.fromBytes(b[off],b[off+1],b[off+2],b[off+3],b[off+4],b[off+5],b[off+6],b[off+7])
    }
    override fun encode(v: Long): ByteArray = Longs.toByteArray(v)
    override fun decodeString(s: String) = s.toLong()
    override val naturalWidth: Int = 8
    override val naturalDefault = 0L
    override fun toString() = "LONG"
    override fun encodeLong(t: Long): Long = t
    override fun decodeLong(l: Long): Long = l
    override val defaultPhysical = this
    override val nullable: PType<Long?> = NPType(this)
    override val MIN_VALUE = Long.MIN_VALUE
    override val MAX_VALUE = Long.MAX_VALUE
  }
  /** See [LongLexicoder]. The first byte appears to store length information: between 1 and 9 bytes. */
  object LONG_VARIABLE : PType<Long>(), Comparator<Long> by serialNaturalOrder() {
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
    override val MIN_VALUE = Long.MIN_VALUE
    override val MAX_VALUE = Long.MAX_VALUE
  }
  object BOOLEAN : PType<Boolean>(), Comparator<Boolean> by serialNaturalOrder() {
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
    override val MIN_VALUE = false
    override val MAX_VALUE = true
  }
  /** Encode in terms of long bits. Probably does not preserve order. */
  object DOUBLE : PType<Double>(), Comparator<Double> by serialNaturalOrder() {
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
    override val MIN_VALUE = Double.NEGATIVE_INFINITY
    override val MAX_VALUE = Double.POSITIVE_INFINITY
  }
  /** See [DoubleLexicoder]. The first byte appears to store length information: between 1 and 9 bytes. */
  object DOUBLE_VARIABLE : PType<Double>(), Comparator<Double> by serialNaturalOrder() {
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
    override val MIN_VALUE = Double.NEGATIVE_INFINITY
    override val MAX_VALUE = Double.POSITIVE_INFINITY
  }
  /** UTF8 string encoding */
  object STRING : PType<String>(), Comparator<String> by serialNaturalOrder() {
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
    override val MIN_VALUE: String = ""
    override val MAX_VALUE: String = Character.MAX_VALUE.toString().repeat(5) // weird. Hopefully okay
  }
//  object DATETIME : PType<DateTime>() {
//    val lex = DateTimeLexicoder()
//    override fun decode(b: ByteArray, offset: Int, len: Int): DateTime = lex.decode(b, offset, len)
//    override fun encode(v: DateTime?): ByteArray = lex.encode(v)
//  }
  /** 4 byte constant width. Probably does not preserve order */
  object FLOAT : PType<Float>(), Comparator<Float> by serialNaturalOrder() {
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
    override val MIN_VALUE = Float.NEGATIVE_INFINITY
    override val MAX_VALUE = Float.POSITIVE_INFINITY
  }
  /** See [FloatLexicoder]. The first byte appears to store length information: between 1 and 5 bytes. */
  object FLOAT_VARIABLE : PType<Float>(), Comparator<Float> by serialNaturalOrder() {
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
    override val MIN_VALUE = Float.NEGATIVE_INFINITY
    override val MAX_VALUE = Float.POSITIVE_INFINITY
  }


//  object BYTE_ARRAY : PType<ByteArray>() {
//
//  }
}

