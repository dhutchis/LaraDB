package edu.washington.cs.laragraphulo.api

import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.LexicoderPlus
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.warn
import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.slf4j.Logger
import java.io.IOException
import java.io.ObjectInputStream
import java.io.Serializable
import java.util.*
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty


// ======================= HELPER FUNCTIONS

fun <E> Collection<E>.disjoint(other: Collection<E>): Boolean {
  return this.none { other.contains(it) }
}

/**
 * Return a NameTuple with the same keys but the values set to the default values.
 */
fun Tuple.copyDefault(ns: Schema): Tuple {
  require(this.keys == (ns.keys + ns.vals).toSet())
  return this.mapValues { (attr, value) ->
    ns.getValue(attr)?.default ?: value
  }
}


// ======================= ATTRIBUTES
const val ZERO_BYTE: Byte = 0
val SINGLE_ZERO = byteArrayOf(ZERO_BYTE) // sort null values first

/** Would this come in handy? Uses an extra byte to flag null values. Probably not. */
class NullLexicoder<T>(
    private val lexicoder: LexicoderPlus<T>
) : AbstractLexicoder<T?>(), Serializable {
  override fun encode(v: T?): ByteArray {
    return if (v == null) {
      SINGLE_ZERO
    } else {
      val e = lexicoder.encode(v)
      val r = ByteArray(e.size+1)
      r[0] = 1
      System.arraycopy(e,0,r,1,e.size)
      r
    }
  }

  override fun decodeUnchecked(b: ByteArray, offset: Int, len: Int): T? {
    return if (b[0] == ZERO_BYTE) null
    else lexicoder.decode(b, offset+1, len-1)
  }
}


interface Attribute<T> : Comparable<Attribute<T>>, Serializable {
  val name: Name
  val type: LType<T>
  fun withNewName(name: Name) = Attribute(name, type)
  operator fun component1() = name
  operator fun component2() = type

  fun defaultPhysical(): PAttribute<T> = PAttribute(name, type.defaultPhysical)

  companion object {
    operator fun <T> invoke(name: Name, type: LType<T>): Attribute<T> = AttributeImpl(name, type)
  }

  open class AttributeImpl<T>(
      override val name: Name,
      override val type: LType<T>
  ) : Attribute<T>, Serializable {

    override fun toString(): String = "Attr($name, $type)"

    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (other?.javaClass != javaClass) return false

      other as Attribute<*>

      if (name != other.name) return false
      if (type != other.type) return false

      return true
    }

    override fun hashCode(): Int {
      var result = name.hashCode()
      result = 31 * result + type.hashCode()
      return result
    }

    /** Careful: this returns 0 on objects that are not equal */
    override fun compareTo(other: Attribute<T>): Int = name.compareTo(other.name)
  }
}

interface ValAttribute<T> : Attribute<T> {
  val default: T
  override fun withNewName(name: Name) = ValAttribute(name, type, default)
  operator fun component3() = default

  override fun defaultPhysical(): PValAttribute<T> = PValAttribute(name, type.defaultPhysical, default)

  companion object {
    operator fun <T> invoke(name: Name, type: LType<T>, default: T): ValAttribute<T> = ValAttributeImpl(name, type, default)
  }

  open class ValAttributeImpl<T>(
      name: Name,
      type: LType<T>,
      override val default: T
  ) : Attribute.AttributeImpl<T>(name, type), ValAttribute<T> {

    override fun toString(): String = "ValAttr($name, $type, default=$default)"

    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (other?.javaClass != javaClass) return false
      if (!super.equals(other)) return false

      other as ValAttribute<*>

      if (default != other.default) return false

      return true
    }

    override fun hashCode(): Int {
      var result = super.hashCode()
      result = 31 * result + (default?.hashCode() ?: 0)
      return result
    }
  }
}


// ======================= SCHEMA

open class Schema(
  val keys: List<Attribute<*>>,
  val vals: List<ValAttribute<*>>
) : Comparator<Tuple>, Serializable {
  init {
    val kns = keys.map(Attribute<*>::name)
    val vns = vals.map(ValAttribute<*>::name)
    require(kns.let { it.size == it.toSet().size }) {"there is a duplicate key attribute name: $keys"}
    require(vns.let { it.size == it.toSet().size }) {"there is a duplicate value attribute name: $vals"}
    require(kns.disjoint(vns)) { "keys and vals overlap: $keys, $vals" }
  }

  open operator fun get(n: Name): Attribute<*>? =
      keys.find { it.name == n } ?: vals.find { it.name == n }

  open fun getValue(n: Name): ValAttribute<*>? =
      vals.find { it.name == n }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Schema

    if (keys != other.keys) return false
    if (vals != other.vals) return false

    return true
  }

  override fun hashCode(): Int {
    var result = keys.hashCode()
    result = 31 * result + vals.hashCode()
    return result
  }

  open operator fun component1() = keys
  open operator fun component2() = vals

  override fun toString(): String = "Schema(keys=$keys, vals=$vals)"

  override fun compare(t1: Tuple, t2: Tuple): Int = compareByKey(keys, t1, t2)

  /** Default physical schema.
   * Todo: make other physical schemas. We may not always want to put all the keys in the row, for example if we need a coarser grouping.
   * */
  open fun defaultPSchema(): PSchema {
    val pkeys = keys.map(Attribute<*>::defaultPhysical)
    val pvals = vals.map(ValAttribute<*>::defaultPhysical)
    return PSchema(row = pkeys, pvals = pvals)
  }

  companion object {
    fun compareByKey(keys: List<Attribute<*>>, t1: Tuple, t2: Tuple): Int {
      keys.forEach {
        val n = it.name
        assert(n in t1 && n in t2)
        val c = it.type.compareUnchecked(t1[n], t2[n])
        if (c != 0) return c
      }
      return 0
    }
  }
}

data class KeyComparator(
    val keys: List<Attribute<*>>
) : Comparator<Tuple>, Serializable {
  override fun compare(t1: Tuple, t2: Tuple): Int = Schema.compareByKey(keys, t1, t2)
}

// ======================= TUPLE





// ======================= UDFs

/**
 * Must return default values when passed default values, for any key.
 */
open class ExtFun(
    val name: String,
    /** Schema of tuples produced by the Ext as a function of the parent schema. New key attributes are appended to those of the parent. */
    val extSchema: (Schema) -> Schema,
    val extFun: (Tuple) -> List<Tuple> // not included in equals(); use name instead
) : Serializable {
  constructor(name: String, extSchema: Schema, extFun: (Tuple) -> List<Tuple>)
  : this(name, {extSchema}, extFun)

  override fun toString(): String = "ExtFun($name, extSchema=$extSchema)"
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    other as ExtFun
    if (name != other.name) return false
    if (extSchema != other.extSchema) return false
    return true
  }
  override fun hashCode(): Int {
    var result = name.hashCode()
    result = 31 * result + extSchema.hashCode()
    return result
  }


}

/**
 * Must return default values when passed default values, for any key.
 */
open class MapFun(
    name: String,
    /** Value attributes of tuples produced by the Map as a function of the parent schema. */
    val mapValues: (Schema) -> List<ValAttribute<*>>,
    val mapFun: (Tuple) -> Tuple
) : ExtFun(name, extSchema = {Schema(listOf(), mapValues(it))}, extFun = { tuple -> listOf(mapFun(tuple)) }) {
  constructor(name: String, mapValues: List<ValAttribute<*>>, mapFun: (Tuple) -> Tuple)
  : this(name, {mapValues}, mapFun)

  override fun toString(): String = "MapFun($name, mapValues=$mapValues)"
}

class FilterFun(
    name: String,
    val filterFun: (Tuple) -> Boolean
) : MapFun(name, mapValues = {it.vals}, mapFun = { if (filterFun(it)) it else mapOf<String,Any?>() }) {
  override fun toString(): String = "FilterFun($name)"
}



data class PlusFun<T>(
    val name: String,
    val identity: T,
    val plus: (T, T) -> T
) : Serializable {
  fun verifyIdentity(a: T = identity) {
    check(plus(a, identity) == a && plus(identity,a) == a) {"Value $a violates the identity requirement of plus for identity $identity"}
  }

  override fun toString(): String = "PlusFun($name, identity=$identity)"
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as PlusFun<*>

    if (name != other.name) return false
    if (identity != other.identity) return false

    return true
  }

  override fun hashCode(): Int {
    var result = name.hashCode()
    result = 31 * result + (identity?.hashCode() ?: 0)
    return result
  }


  companion object {
    /** Wraps a function to have an identity. */
    inline fun <T> withIdentity(name: String, id: T, crossinline plusFun: (T,T) -> T) = PlusFun(name, id) { a, b ->
      when {
        a == id -> b
        b == id -> a
        else -> plusFun(a,b)
      }
    }

    /** Wraps a function to have identity null (that is zero-sum-free). */
    inline fun <T : Any> withNullIdentity(name: String, crossinline plusFun: (T, T) -> T): PlusFun<T?> {
      return PlusFun<T?>(name, null) { a, b ->
        when {
          a == null -> b
          b == null -> a
          else -> plusFun(a,b)
        }
      }
    }

    const val PLUS_ERROR_FUN = "PlusErrorFun"

    /** Use this when you know that summation will never occur. Throws an error when summing two non-identities. */
    fun <T> plusErrorFun(id: T) = PlusFun(PLUS_ERROR_FUN, id) { a, b ->
      when {
        a == id -> b
        b == id -> a
        else -> throw IllegalStateException("no plus function defined for this attribute, yet non-identity ($id) values $a and $b are to be added")
      }
    }
  }
}



data class TimesFun<T1,T2,T3>(
    val name: String,
    val leftAnnihilator: T1,
    val rightAnnihilator: T2, // (PType<T1>, PType<T2>) -> PType<T3>
    val resultType: LType<T3>,
    val times: (T1, T2) -> T3
) : Serializable {
  @Transient val resultZero: T3 = times(leftAnnihilator, rightAnnihilator)

  fun verifyAnnihilator(a: T1 = leftAnnihilator, b: T2 = rightAnnihilator) {
    check(times(a, rightAnnihilator) == resultZero && times(leftAnnihilator,b) == resultZero)
    { "Value $a and $b violate the annihilator requirement of times for annihilators $leftAnnihilator and $rightAnnihilator" }
  }

  @Throws(ClassNotFoundException::class, IOException::class)
  private fun readObject(ois: ObjectInputStream) {
    ois.defaultReadObject()
    // reconstruct resultZero on deserialization
    val f = TimesFun::class.java.declaredFields.find { it.name == this::resultZero.name }!!
    f.isAccessible = true
    f.set(this, times(leftAnnihilator, rightAnnihilator))
  }

  override fun toString(): String = "TimesFun(name='$name', leftAnnihilator=$leftAnnihilator, rightAnnihilator=$rightAnnihilator, resultType=$resultType)"
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as TimesFun<*, *, *>

    if (name != other.name) return false
    if (leftAnnihilator != other.leftAnnihilator) return false
    if (rightAnnihilator != other.rightAnnihilator) return false
    if (resultType != other.resultType) return false

    return true
  }

  override fun hashCode(): Int {
    var result = name.hashCode()
    result = 31 * result + (leftAnnihilator?.hashCode() ?: 0)
    result = 31 * result + (rightAnnihilator?.hashCode() ?: 0)
    result = 31 * result + resultType.hashCode()
    return result
  }

  companion object {
    /** Wraps a function to have these annihilators. */
    inline fun <T1, T2, T3> withAnnihilators(name: String,
        leftAnnihilator: T1, rightAnnihilator: T2,
        resultType: LType<T3>,
        crossinline timesFun: (T1, T2) -> T3
    ): TimesFun<T1, T2, T3> {
      val resultZero = timesFun(leftAnnihilator, rightAnnihilator)
      return TimesFun(name, leftAnnihilator, rightAnnihilator, resultType) { a, b ->
        if (a == leftAnnihilator || b == rightAnnihilator) resultZero else timesFun(a, b)
      }
    }

    /** Wraps a function to have null annihilators (with zero product property). */
    inline fun <T1, T2, T3> withNullAnnihilators(name: String,
        resultType: LType<T3?>,
        crossinline timesFun: (T1, T2) -> T3
    ): TimesFun<T1?, T2?, T3?> = TimesFun<T1?, T2?, T3?>(name, null, null, resultType) { a, b ->
      if (a == null || b == null) null else timesFun(a, b)
    }
  }
}

// use a map from Java class to most common PType for that class


// ======================= OPERATORS

data class KeyValue(val key: Key, val value: Value) : Serializable {
  constructor(kv: Pair<Key, Value>): this(kv.first, kv.second)
  constructor(kv: Map.Entry<Key, Value>): this(kv.key, kv.value)

  fun toTuple(ps: PSchema): Tuple = TupleByKeyValue(ps, key, value)
}

interface AccumuloLikeIterator<K,T> : PeekingIterator<T> {
  override fun remove() = throw UnsupportedOperationException("remove is not supported")

  fun seek(seek: K)
  fun deepCopy(env: IteratorEnvironment): AccumuloLikeIterator<K,T>

//  /** @return null if it is not safe to serialize state */
//  fun serializeState(): ByteArray?
//  // an init() function?

  /** Return the next value. This is the same value that [peek] returns prior to calling next. */
  override fun next(): T
  /** Peek at the next value; do not advance the iteration */
  override fun peek(): T

  /** From [org.apache.accumulo.core.iterators.SortedKeyValueIterator] */
  fun init(options: Map<String, String>, env: IteratorEnvironment): AccumuloLikeIterator<K,T> = this
}

interface KeyValueIterator : AccumuloLikeIterator<SeekKey,KeyValue> {
  override fun deepCopy(env: IteratorEnvironment): KeyValueIterator
  override fun init(options: Map<String, String>, env: IteratorEnvironment): KeyValueIterator = this
}

interface TupleIterator : AccumuloLikeIterator<TupleSeekKey, Tuple> {
  override fun init(options: Map<String, String>, env: IteratorEnvironment): TupleIterator = this
  /** Requirement: `X.deepCopy(_) == X` */
  override fun deepCopy(env: IteratorEnvironment): TupleIterator
  companion object {
    val EMPTY = object : TupleIterator {
      override fun hasNext() = false
      /** Seek this iterator to a particular TupleSeekKey. The next calls to next(), peek(), and hasNext() will reflect the newly seeked state. */
      override fun seek(seek: TupleSeekKey) {}
      override fun next() = throw UnsupportedOperationException("empty TupleIterator")
      override fun peek() = throw UnsupportedOperationException("empty TupleIterator")
      override fun deepCopy(env: IteratorEnvironment) = this
    }
  }

  class DataTupleIterator(val comp: Comparator<Tuple>, collection: Iterable<Tuple>) : TupleIterator {
    val list: List<Tuple> = collection.sortedWith(comp)
    var iter = list.iterator().peeking()
    override fun seek(seek: TupleSeekKey) {
      iter = seek.range.restrict(comp, list.iterator().peeking())
    }

    override fun hasNext(): Boolean = iter.hasNext()
    override fun next(): Tuple = iter.next()
    override fun peek(): Tuple = iter.peek()
    // no deep copying necessary because collection is materialized
    override fun deepCopy(env: IteratorEnvironment) = DataTupleIterator(comp, list)
  }
  /** Only allows a single iteration. No deep copying. */
  class DataTupleIteratorOnce(val comp: Comparator<Tuple>, var iter: PeekingIterator<Tuple>) : TupleIterator {
    var seeked = false
    override fun seek(seek: TupleSeekKey) {
      if (seeked) logger.warn{"seeking more than once on $this"}
      iter = seek.range.restrict(comp, iter)
      seeked = true
    }
    override fun hasNext(): Boolean = iter.hasNext()
    override fun next(): Tuple = iter.next()
    override fun peek(): Tuple = iter.peek()
    override fun deepCopy(env: IteratorEnvironment) = throw UnsupportedOperationException("cannot deepCopy this class; only allows a single iteration through")
    companion object : Loggable {
      override val logger: Logger = logger<DataTupleIteratorOnce>()
    }
  }
}


/** Used to stage values during construction.
 * The [f] will be invoked when this property is first read, unless this property is set first. */
class Staged<T>(f0: () -> T) : ReadWriteProperty<Any?, T> {
  private var f: (() -> T)? = f0
  private var _value: Any? = UNINITIALIZED_VALUE

  override fun getValue(thisRef: Any?, property: KProperty<*>): T {
    if (f != null) { _value = f!!(); f = null }
    @Suppress("UNCHECKED_CAST")
    return _value as T
  }

  override fun setValue(thisRef: Any?, property: KProperty<*>, value: T) {
    f = null
    _value = value
  }

  fun isInitialized(): Boolean = f != null
  fun reinitialize(f0: () -> T) {
    f = f0
    _value = UNINITIALIZED_VALUE
  }

  // toString does not force
  override fun toString(): String = _value.toString()
  // unsure about equals and hashCode

//  private fun writeReplace(): Any = InitializedLazyImpl(value)

  private object UNINITIALIZED_VALUE {
    override fun toString(): String = "Staged UNINITIALIZED_VALUE"
  }
}

data class SeekKey(
    val range: org.apache.accumulo.core.data.Range = org.apache.accumulo.core.data.Range(),
    val families: Collection<ABS> = Collections.emptyList(),
    val inclusive: Boolean = false
) {
  fun toTupleSeekKey(ps: PSchema): TupleSeekKey =
      TupleSeekKey(
//          MyRange.fromGuava(range) { k -> TupleByKeyValue(ps, k, null) },
          MyRange.fromAccumulo(range).transform { k -> TupleByKeyValue(ps, k, null) },
          this.families, this.inclusive
      )
}

data class TupleSeekKey(
    val range: MyRange<Tuple> = MyRange.all(),
    val families: Collection<ABS> = Collections.emptyList(),
    val inclusive: Boolean = false
) {
  fun toSeekKey(ps: PSchema): SeekKey =
      SeekKey(
          MyRange.toAccumulo(range.transform { tuple -> ps.encodeToKeyValue(tuple).key }),
          families, inclusive
      )
}
