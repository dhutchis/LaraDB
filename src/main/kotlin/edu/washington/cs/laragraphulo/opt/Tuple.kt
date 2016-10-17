package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.*
import edu.washington.cs.laragraphulo.*
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.slf4j.Logger
import java.io.Serializable
import java.lang.reflect.Constructor
import java.util.*
import kotlin.comparisons.compareBy
import kotlin.reflect.jvm.javaConstructor

data class KeyValue(val key: Key, val value: Value) : Serializable {
  constructor(kv: Pair<Key,Value>): this(kv.first, kv.second)
  constructor(kv: Map.Entry<Key,Value>): this(kv.key, kv.value)
}

data class FullValue(
    val value: ArrayByteSequence,
    val visibility: ArrayByteSequence,
    val timestamp: Long
) : Serializable

interface TupleKey : Comparable<TupleKey>, Serializable {
  val keys: List<ArrayByteSequence>
  val family: ArrayByteSequence

  fun toKey(apKeySchema: APKeySchema): Key

//  operator fun get(apSchema: APSchema, name: String): ArrayByteSequence? {
//    val idx = apSchema.keyNames.indexOf(name)
//    return if (idx == -1) null else this.keys[idx]
//  }
}

open class TupleKeyImpl(
    keys: List<ArrayByteSequence>,
    override val family: ArrayByteSequence
) : TupleKey {
  override val keys: List<ArrayByteSequence> = ImmutableList.copyOf(keys)

  override fun compareTo(other: TupleKey): Int {
    val thisIter = keys.iterator()
    val otherIter = other.keys.iterator()
    while (thisIter.hasNext() && otherIter.hasNext()) {
      val c = thisIter.next().compareTo(otherIter.next())
      if (c != 0) return c
    }
    return when {
      thisIter.hasNext() -> 1
      otherIter.hasNext() -> -1
      // is this where we should compare family?
      else -> family.compareTo(other.family)
    }
  }

  /** Convert this TupleKey to a Key filled up to the CQ with the dapNames, family, and lapNames. */
  override fun toKey(apKeySchema: APKeySchema): Key {
    /** Only when [IntRange.step] is 0 */
    fun IntRange.size(): Int = this.endInclusive - this.first + 1

    /** @return Array, offset, length */
    fun arrayFromParts(keyRange: IntRange): Triple<ByteArray,Int,Int> =
        when {
          keyRange.isEmpty() -> Triple(ByteArray(0),0,0)
          keyRange.size() == 1 -> keys[keyRange.first].let { Triple(it.backingArray, it.offset(), it.length()) }
        // general case: copy
          else -> {
            val dapList = keys.slice(keyRange)
            val rowLen = dapList.sumBy { it.length() }
            val rowArr = ByteArray(rowLen)
            var p = 0
            for (seq in dapList) {
              val len = seq.length()
              System.arraycopy(seq.backingArray, seq.offset(), rowArr, p, len)
              p += len
            }
            Triple(rowArr, 0, rowLen)
          }
        }

    // an optimization is possible whereby we check to see if the dapList's ArrayByteSequences
    // all reference the same array and form a contiguous block of the array
    val (rowArr, rowOff, rowLen) = try {
      arrayFromParts(apKeySchema.dapRange)
    } catch (e: IndexOutOfBoundsException) { throw IllegalStateException("index problem; schema is $apKeySchema; tuple is $keys", e) }

    // column qualifier prefix
    val (lapArr, lapOff, lapLen) = arrayFromParts(apKeySchema.lapRange)
    // this could be optimized in the case of singleton vals

    return if (rowOff == 0 && rowLen == rowArr.size &&
        family.isContiguousArray() &&
        lapOff == 0 && lapLen == lapArr.size)
      Key(rowArr, family.backingArray, lapArr, EMPTY_B, Long.MAX_VALUE, false, false) // no copy
    else
      Key(rowArr, rowOff, rowLen,
          family.backingArray, family.offset(), family.length(),
          lapArr, lapOff, lapLen,
          EMPTY_B, 0, 0, Long.MAX_VALUE)
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as TupleKeyImpl

    if (family != other.family) return false
    if (keys != other.keys) return false

    return true
  }

  override fun hashCode(): Int{
    var result = family.hashCode()
    result = 31 * result + keys.hashCode()
    return result
  }

  override fun toString(): String = "TupleKeyImpl(keys=$keys, family=$family)"
}


/** Be wary of mutating anything. Try to keep it immutable. */
interface Tuple : TupleKey {
//  val keys: List<ArrayByteSequence>
//  val family: ArrayByteSequence
  /** At a minimum, this should contain a mapping from the empty string to a FullValue. */
  val vals: ListMultimap<ArrayByteSequence, FullValue>

  fun toKeyValues(apKeySchema: APKeySchema): List<KeyValue>

//  override operator fun get(apSchema: APSchema, name: String): ArrayByteSequence? {
//    val res = super.get(apSchema, name)
//    return if (res == null) vals[ArrayByteSequence(name.toByteArray())]. else res
//  }
}

class TupleImpl(
    keys: List<ArrayByteSequence>,
    family: ArrayByteSequence,
    vals: ListMultimap<ArrayByteSequence, FullValue>
) : TupleKeyImpl(keys, family), Tuple {
  override val vals: ImmutableListMultimap<ArrayByteSequence, FullValue> = ImmutableListMultimap.copyOf(vals)

  /**
   * Convert this Tuple to a list of KeyValues.
   * To guarantee sortedness, either change the implementation here or use Collections.sort afterward, with a key comparator
   *
   * Todo: this function could be re-formulated as a generator.
   */
  override fun toKeyValues(apKeySchema: APKeySchema): List<KeyValue> {
    /** Only when [IntRange.step] is 0 */
    fun IntRange.size(): Int = this.endInclusive - this.first + 1

    /** @return Array, offset, length */
    fun arrayFromParts(keyRange: IntRange): Triple<ByteArray,Int,Int> =
        when {
          keyRange.isEmpty() -> Triple(ByteArray(0),0,0)
          keyRange.size() == 1 -> keys[keyRange.first].let { Triple(it.backingArray, it.offset(), it.length()) }
        // general case: copy
          else -> {
            val dapList = keys.slice(keyRange)
            val rowLen = dapList.sumBy { it.length() }
            val rowArr = ByteArray(rowLen)
            var p = 0
            for (seq in dapList) {
              val len = seq.length()
              System.arraycopy(seq.backingArray, seq.offset(), rowArr, p, len)
              p += len
            }
            Triple(rowArr, 0, rowLen)
          }
        }

    // an optimization is possible whereby we check to see if the dapList's ArrayByteSequences
    // all reference the same array and form a contiguous block of the array
    val (rowArr, rowOff, rowLen) = arrayFromParts(apKeySchema.dapRange)

    // column qualifier prefix
    val (lapArr, lapOff, lapLen) = arrayFromParts(apKeySchema.lapRange)
    // this could be optimized in the case of singleton vals

    val retList = ArrayList<KeyValue>(vals.size())
    for ((valName, fullValueList) in vals.asMap().entries) {
      val cqArr = ByteArray(lapLen + valName.length())
      System.arraycopy(lapArr, lapOff, cqArr, 0, lapLen)
      System.arraycopy(valName.backingArray, valName.offset(), cqArr, lapLen, valName.length())

      for ((valueArr, visibility, timestamp) in fullValueList) {
        val key =
            if (rowOff == 0 && rowLen == rowArr.size &&
                family.isContiguousArray() && visibility.isContiguousArray())
              Key(rowArr, family.backingArray, cqArr, visibility.backingArray, timestamp, false, false) // no copy
            else
              Key(rowArr, rowOff, rowLen,
                  family.backingArray, family.offset(), family.length(),
                  cqArr, 0, cqArr.size,
                  visibility.backingArray, visibility.offset(), visibility.length(),
                  timestamp
              ) // copies data, sadly. 2.0 api may get around this
        val value = valueArr.backingArray.let {
          if (valueArr.offset() == 0 && valueArr.length() == it.size) Value(it, false) // no copy
          else Value(it, valueArr.offset(), valueArr.length()) // copy
        }
        retList.add(KeyValue(key, value))
      }
    }
    return retList
  }

  override fun toString(): String = "Tuple(keys=$keys, family=$family, vals=$vals)"
  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    if (!super.equals(other)) return false

    other as TupleImpl

    if (vals != other.vals) return false

    return true
  }
  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + vals.hashCode()
    return result
  }
}


fun ArrayByteSequence.isContiguousArray() = this.offset() == 0 && this.length() == this.backingArray.size

val EMPTY_B = byteArrayOf()
val EMPTY = ArrayByteSequence(EMPTY_B,0,0)




class KeyValueToTuple(
    private val kvIter: PeekingIterator<KeyValue>,
    val apKeySchema: APKeySchema,
    val widthSchema: WidthSchema
): Iterator<Tuple> {
  init {
    require(widthSchema.widths.size >= apKeySchema.keyNames.size) {"bad widthSchema $widthSchema for schema $apKeySchema"}
  }

  val keyComparator = compareBy<KeyValue, Key>(
      KeyValueComparatorToQualifierPrefix(
          widthSchema.widths.subList(apKeySchema.dapNames.size, apKeySchema.dapNames.size+ apKeySchema.lapNames.size).map {
            if (it == -1) throw UnsupportedOperationException("not supporting variable-length key attributes yet"); it }.sum()
      )) { it.key }


  private lateinit var rowIter: OneRowIterator<KeyValue>
  private lateinit var tupleKey: TupleKey
  private lateinit var keyListBuilt: List<ArrayByteSequence>
  private var valNamePos: Int = -1
  private lateinit var family: ArrayByteSequence

  companion object {

    /** @return null on failure to parse */
    fun readKeyFromTop(apKeySchema: APKeySchema, widthSchema: WidthSchema, k: Key): TupleKey? {
      val keyList = ImmutableList.builder<ArrayByteSequence>()
      /** @return the position of the first byte not read, or -1 if this is a bad tuple */
      fun addToList(bs: ByteSequence, off: Int, len: Int, allowVariableLast: Boolean): Int {
        assert(bs.isBackedByArray)
        var p = 0
        for (i in off..off + len - 1) {
          var width = widthSchema.widths[i]
          require(width != -1 || (allowVariableLast && i == off + len - 1)) { "Variable width not allowed here. Widths are ${widthSchema.widths}. Schema is $apKeySchema" }
          if (width == -1) {
            width = bs.length() - p
          } else if (p + width > bs.length()) {
            println("Warning: Dropping Tuple: bad key $k for schema $apKeySchema and widths ${widthSchema.widths}")
            return -1
          }
          keyList.add(ArrayByteSequence(bs.backingArray, bs.offset() + p, width))
          p += width
        }
        return p
      }

      // fill the dapNames from the row
      val row = k.rowData
      val tmp = addToList(row, 0, apKeySchema.dapNames.size, true) // don't care about the bytes remaining after reading the row
      if (tmp == -1) return null

      // fill the lapNames from the cq
      val cqFirst = k.columnQualifierData
      val valNamePos = addToList(cqFirst, apKeySchema.dapNames.size, apKeySchema.lapNames.size, false)
      if (valNamePos == -1) return null

      assert(k.columnFamilyData is ArrayByteSequence)
      val family = k.columnFamilyData as ArrayByteSequence

      val keyListBuilt = keyList.build()
      return TupleKeyImpl(keyListBuilt, family)
    }

  }

  private fun readKeysFromTop() {
    while(kvIter.hasNext()) {
      rowIter = OneRowIterator(keyComparator, kvIter)
      val keyList = ImmutableList.builder<ArrayByteSequence>()
      val firstKV = rowIter.peek()

      /** @return the position of the first byte not read, or -1 if this is a bad tuple */
      fun addToList(bs: ByteSequence, off: Int, len: Int, allowVariableLast: Boolean): Int {
        assert(bs.isBackedByArray)
        var p = 0
        for (i in off..off + len - 1) {
          var width = widthSchema.widths[i]
          require(width != -1 || (allowVariableLast && i == off + len - 1)) { "Variable width not allowed here. Widths are ${widthSchema.widths}" }
          if (width == -1) {
            width = bs.length() - p
          } else if (p + width > bs.length()) {
            println("Warning: Dropping Tuple: bad key ${firstKV.key} for schema $apKeySchema and widths ${widthSchema.widths}")
            while (rowIter.hasNext()) rowIter.next() // drain tuple
            return -1
          }
          keyList.add(ArrayByteSequence(bs.backingArray, bs.offset() + p, width))
          p += width
        }
        return p
      }

      // fill the dapNames from the row
      val row = firstKV.key.rowData
      val tmp = addToList(row, 0, apKeySchema.dapNames.size, true) // don't care about the bytes remaining after reading the row
      if (tmp == -1) continue

      // fill the lapNames from the cq
      val cqFirst = firstKV.key.columnQualifierData
      valNamePos = addToList(cqFirst, apKeySchema.dapNames.size, apKeySchema.lapNames.size, false)
      if (valNamePos == -1) continue

      assert(firstKV.key.columnFamilyData is ArrayByteSequence)
      family = firstKV.key.columnFamilyData as ArrayByteSequence

      keyListBuilt = keyList.build()

      break
    }
  }

  override fun hasNext(): Boolean {
    if (valNamePos == -1)
      readKeysFromTop()
    return kvIter.hasNext()
  }

  override fun next(): Tuple {
    if (!hasNext())
      throw NoSuchElementException()

    // vals
    val vals = ImmutableListMultimap.builder<ArrayByteSequence, FullValue>()
    do {
      val kv = rowIter.next()
      val cq = kv.key.columnQualifierData
      val valName = ArrayByteSequence(cq.backingArray, valNamePos, cq.length() - valNamePos)
      val tmp = kv.value.get()
      assert(kv.key.columnVisibilityData is ArrayByteSequence)
      val valValue = FullValue(ArrayByteSequence(tmp, 0, tmp.size),
          kv.key.columnVisibilityData as ArrayByteSequence, kv.key.timestamp)
      vals.put(valName, valValue)
    } while (rowIter.hasNext())

    valNamePos = -1

    return TupleImpl(keyListBuilt, family, vals.build())
  }
}

class KeyValueToTupleIterator(
    private val keyValueIterator: KeyValueIterator,
    val apKeySchema: APKeySchema,
    val widthSchema: WidthSchema
) : TupleIterator {
  private var innerIterator: PeekingIterator<Tuple> = Iterators.peekingIterator(KeyValueToTuple(keyValueIterator, apKeySchema, widthSchema))

  override fun seek(seek: TupleSeekKey) {
    val sk = seek.toSeekKey(apKeySchema)
    keyValueIterator.seek(sk)
    innerIterator = Iterators.peekingIterator(KeyValueToTuple(keyValueIterator, apKeySchema, widthSchema))
  }

  override fun deepCopy(env: IteratorEnvironment): KeyValueToTupleIterator {
    return KeyValueToTupleIterator(keyValueIterator.deepCopy(env), apKeySchema, widthSchema)
  }
  override fun hasNext(): Boolean = innerIterator.hasNext()
  override fun peek(): Tuple = innerIterator.peek()
  override fun next(): Tuple = innerIterator.next()
  override fun serializeState(): ByteArray {
    // todo: return the last tuple emitted, so that we can restore the state
    // by seeking to the tuple immediately after the last one emitted
    return EMPTY_B
  }
}

//val con: Constructor<KeyValueToTupleIterator>? = ::KeyValueToTupleIterator.javaConstructor

class TupleToKeyValueIterator(
    private val tupleIterator: TupleIterator,
    val apKeySchema: APKeySchema,
    val widthSchema: WidthSchema
) : KeyValueIterator {
  private var iterFromLastTuple = Iterators.peekingIterator(Collections.emptyIterator<KeyValue>())

  private fun findTop() {
    while (!iterFromLastTuple.hasNext() && tupleIterator.hasNext()) {
      iterFromLastTuple = Iterators.peekingIterator(tupleIterator.next().toKeyValues(apKeySchema).iterator())
    }
  }

  override fun hasNext(): Boolean {
    findTop()
    return iterFromLastTuple.hasNext()
  }

  override fun next(): KeyValue {
    findTop()
    if (iterFromLastTuple.hasNext())
      return iterFromLastTuple.next()
    throw NoSuchElementException()
  }

  override fun peek(): KeyValue {
    findTop()
    if (iterFromLastTuple.hasNext())
      return iterFromLastTuple.peek()
    throw NoSuchElementException()
  }

  override fun deepCopy(env: IteratorEnvironment): TupleToKeyValueIterator {
    return TupleToKeyValueIterator(tupleIterator.deepCopy(env), apKeySchema, widthSchema)
  }

  override fun seek(seek: SeekKey) {
    val tsk = seek.toTupleSeekKey(apKeySchema, widthSchema)
    logger.debug{"seek: $seek with schema: $apKeySchema to result in $tsk"}
    tupleIterator.seek(tsk)
  }

  override fun serializeState(): ByteArray? {
    // todo: return the last emitted
    return if (iterFromLastTuple.hasNext()) null else EMPTY_B
  }

  companion object : Loggable {
    override val logger: Logger = logger<TupleToKeyValueIterator>()
  }
}

