package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.api.TupleOp.*
import edu.washington.cs.laragraphulo.encoding.escapeAndJoin
import edu.washington.cs.laragraphulo.encoding.splitAndUnescape
import edu.washington.cs.laragraphulo.opt.EMPTY_B
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.hadoop.io.Text
import org.slf4j.Logger
import java.nio.ByteBuffer
import java.text.DateFormat
import java.text.SimpleDateFormat


//fun TupleOp.getBaseTables(): Set<Table> = when(this) {
//  is Load -> setOf(this.table)
//  is Ext -> this.parent.getBaseTables()
//  is Empty -> setOf()
//  is MergeUnion0 -> this.p1.getBaseTables() + this.p2.getBaseTables()
//  is Rename -> this.p.getBaseTables()
//  is Sort -> this.p.getBaseTables()
//  is MergeJoin -> this.p1.getBaseTables() + this.p2.getBaseTables()
//  is LoadData -> setOf()
//}

fun TupleOp.instantiateLoadOnce(tableMap: Map<Table, Iterator<Tuple>>): TupleOp = this.transform {
  when (it) {
    is Load -> {
      require(it.table in tableMap) { "Attempt to lower a TupleOp stack but no SKVI given for ${it.table}" }
      LoadOnce(it.resultSchema, tableMap[it.table]!!)
    }
    else -> it
  }
}
fun TupleOp.instantiateLoadTupleIterator(tableMap: Map<Table, TupleIterator>): TupleOp = this.transform {
  when (it) {
    is Load -> {
      require(it.table in tableMap) { "Attempt to lower a TupleOp stack but no SKVI given for ${it.table}" }
      LoadTupleIterator(it.resultSchema, tableMap[it.table]!!)
    }
    else -> it
  }
}
fun TupleOp.instantiateLoad(tableMap: Map<Table, Iterable<Tuple>>): TupleOp = this.transform {
  when (it) {
    is Load -> {
      require(it.table in tableMap) { "Attempt to lower a TupleOp stack but no SKVI given for ${it.table}" }
      LoadData(it.resultSchema, tableMap[it.table]!!)
    }
    else -> it
  }
}
fun TupleOp.disableFullResorts(): TupleOp = this.transform {
  when (it) {
    is Sort -> it.copy(fullResort = false)
    else -> it
  }
}

fun TupleOp.getBaseTables(): Set<Table> = this.fold(setOf<Table>()) { op, parentValues ->
  val opTables = when(op) {
    is Load -> setOf(op.table)
    else -> setOf()
  }
  parentValues.fold(opTables) {a,b -> a+b}
}
fun TupleOp.getWrittenTables(): Set<Table> = this.fold(setOf<Table>()) { op, parentValues ->
  val opTables = when(op) {
    is Store -> setOf(op.table)
    else -> setOf()
  }
  parentValues.fold(opTables) {a,b -> a+b}
}

private var lastTime = 0L
private fun getTime(): Long {
  var t = System.currentTimeMillis()
  if (t <= lastTime) t=lastTime+1 // no duplicates please (need more control for multi-threaded; fine for single-thread)
  lastTime = t
  return t
}
private val dateFormat: DateFormat = SimpleDateFormat("'temp_'yyyyMMdd_HHmmssSSS")
fun genName(): String = dateFormat.format(java.util.Date(getTime()))


/** Create a list of TupleOps in topological order, from the top-most Loads down.
 * Each one contains TupleOps until a Sort (pipeline-breaker).
 * At that point, a Store to a temporary table is added and the next TupleOps read from a Load to that table.
 * If a multi-parent TupleOp stems from the same Load, add a DeepCopy on the p2. */
fun TupleOp.splitPipeline(): List<Store> {
  // maybe keep more information like what tables we need to create, but we could get that information from the Store operators too
  // every pipeline ends in a Store
  val pipelines: MutableList<Store> = java.util.LinkedList()
  /** The last op before the Sort (to be transformed into a Store),
   * mapped to the Load of that Store. */
  val prePipelines: MutableMap<Sort, Load> = HashMap()

  val remaining = this.transformFold(mapOf<Sort,Load>()) { (fromChild, op, curPos, retArr) ->
    when (op) {
      is Sort -> when (curPos) {
        0 -> { // pre-Sort
          if (op in fromChild)
            TransformResult.Stop(fromChild[op]!!, fromChild)
          else TransformResult.Continue(fromChild)
        }
        else -> { // post-Sort
          if (op in prePipelines) { // eliminate repeats of the same pipeline
            TransformResult.Stop(prePipelines[op]!!, fromChild)
          } else {
            val tempTable = genName()
            val pipeline = Store(op, tempTable)
            pipelines += pipeline
            val load = Load(tempTable, op.resultSchema)
            prePipelines += op to load
            TransformResult.Stop(load, retArr[0] + (op to load))
          }
        }
      }
      is Store -> when (curPos) {
        0 -> TransformResult.Continue(fromChild)
        else -> {
          pipelines += op
          val load = Load(op.table, op.resultSchema)
          TransformResult.Stop(load, retArr[0])
        }
      }
      else -> when (curPos) {
        0 ->  if (retArr.isEmpty()) TransformResult.Stop(op, fromChild)
              else TransformResult.Continue(fromChild)
        retArr.size -> TransformResult.Stop(op, retArr[retArr.size-1])
        else -> TransformResult.Continue(retArr[curPos-1])
      }
    }
  }

  if (remaining.replacement !is Load) {
    // switch this to a logger for whatever class this is
    println("WARN: Dead code elimination: $remaining")
  }

  return pipelines
}

// map of Attribute in Schema to PType that implements that attribute
// + where the PType is stored: row, column family, column qualifier, timestamp, value
// we want to create functions that obtain the data for each attribute:
// tuple["t"] --> ptype.decode( key[row_idx] )

interface PAttribute<T> : Attribute<T> {
  override val type: PType<T>
  override operator fun component2(): PType<T> = type
  override fun withNewName(name: Name): PAttribute<T> = PAttribute(name, type)

  fun asPValAttribute() = PValAttribute(name, type, type.naturalDefault)
  @Deprecated("no-op", ReplaceWith(""))
  override fun defaultPhysical(): PAttribute<T> = this

  companion object {
    @JvmName(name = "AttributeImp")
    operator fun <T> invoke(name: Name, type: PType<T>): PAttribute<T> = PAttributeImpl(name, type)
  }

  open class PAttributeImpl<T>(
      name: Name,
      override val type: PType<T>
  ) : Attribute.AttributeImpl<T>(name, type), PAttribute<T> {
    override fun toString(): String {
      return "P" + super.toString()
    }
  }
}

// diamond problem: solved with interfaces: this should also inherit PAttribute<T>
interface PValAttribute<T> : PAttribute<T>, ValAttribute<T> {
  override val type: PType<T>
  override fun component2(): PType<T> = type
  override fun withNewName(name: Name): PValAttribute<T> = PValAttribute(name, type, default)

  @Deprecated("no-op", ReplaceWith(""))
  override fun asPValAttribute() = this
  @Deprecated("no-op", ReplaceWith(""))
  override fun defaultPhysical(): PValAttribute<T> = this

  companion object {
    operator fun <T> invoke(name: Name, type: PType<T>, default: T): PValAttribute<T> = PValAttributeImpl(name, type, default)
  }

  class PValAttributeImpl<T>(
      name: Name,
      override val type: PType<T>,
      default: T
  ) : ValAttribute.ValAttributeImpl<T>(name, type, default), PValAttribute<T> {
    override fun toString(): String {
      return "P" + super.toString()
    }
  }

}

// I am restricting this to one key-value pair per tuple.
// A more relaxed implementation would put the value attributes separately, or even group them based on vertical partitioning.
// See FullValue and KeyValueToTuple for the more advanced multiple key-values per tuple ideas.
class PSchema(
    val row: List<PAttribute<*>> = listOf(),
    val family: List<PAttribute<*>> = listOf(),
    val colq: List<PAttribute<*>> = listOf(),
    val vis: PAttribute<*>? = null,
    val ts: PAttribute<*>? = null,
    val pvals: List<PValAttribute<*>>
) : Schema(
    keys = row + family + colq + (if (vis != null) listOf(vis) else listOf()) +
    (if (ts != null) listOf(ts) else listOf()),
    vals = pvals
    ) {
  val rowNames = row.map(Attribute<*>::name)
  val familyNames = family.map(Attribute<*>::name)
  val colqNames = colq.map(Attribute<*>::name)
  val visName = vis?.name
  val tsName = ts?.name
  val keyNames: List<Name> = keys.map(Attribute<*>::name)
  val valNames = vals.map(Attribute<*>::name)
  val allNames: List<Name> = keyNames + vals.map(Attribute<*>::name)
  init {
  }
  val pkeys = row + family + colq + (if (vis != null) listOf(vis) else listOf()) +
      (if (ts != null) listOf(ts) else listOf())
  val pall = pkeys + pvals
  init {
    require(allNames.size == allNames.toSet().size) {"one of the attributes' names is duplicated; $this"}
  }

  override fun component1(): List<PAttribute<*>> = pkeys
  override fun component2(): List<PValAttribute<*>> = pvals
  override fun get(n: Name): PAttribute<*>? = pkeys.find { it.name == n } ?: pvals.find { it.name == n }
  override fun getValue(n: Name): PValAttribute<*>? = pvals.find { it.name == n }
  @Deprecated("no-op", ReplaceWith(""))
  override fun defaultPSchema(): PSchema = this


  fun encodeToKeyValue(t: Tuple): KeyValue {
    require(keyNames.all { it in t }) {"tuple is missing keys for schema $this; tuple is $t"}
    val r = TupleByKeyValue.encodeJoin(row, t)
    val cf = TupleByKeyValue.encodeJoin(family, t)
    val cq = TupleByKeyValue.encodeJoin(colq, t)
    val cv = if (vis == null) EMPTY_B else TupleByKeyValue.encodeJoin(vis, t)
    val time = ts?.type?.encodeLongUnchecked(t[ts.name]) ?: Long.MAX_VALUE
    val vs = TupleByKeyValue.encodeJoin(pvals, t)

    val k = Key(r, cf, cq, cv, time, false, false) // no copy
    val v = Value(vs, false) // no copy
    return KeyValue(k, v)
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    if (!super.equals(other)) return false

    other as PSchema

    if (row != other.row) return false
    if (family != other.family) return false
    if (colq != other.colq) return false
    if (vis != other.vis) return false
    if (ts != other.ts) return false
    if (pvals != other.pvals) return false

    return true
  }

  override fun hashCode(): Int {
    var result = super.hashCode()
    result = 31 * result + row.hashCode()
    result = 31 * result + family.hashCode()
    result = 31 * result + colq.hashCode()
    result = 31 * result + (vis?.hashCode() ?: 0)
    result = 31 * result + (ts?.hashCode() ?: 0)
    result = 31 * result + pvals.hashCode()
    return result
  }

  override fun toString(): String {
    return "PSchema(row=$row, family=$family, colq=$colq, vis=$vis, ts=$ts, pvals=$pvals)"
  }


}

class TupleByKeyValue(val ps: PSchema, val k: Key, val v: Value?): Map<String,Any?> {
  val map: Map<Name, Lazy<Any?>>
  init {
    val r: Map<Name, Lazy<Any?>> = ps.rowNames.zip(decodeSplit(ps.row, k.rowData as ABS)).toMap()
    val fam = ps.familyNames.zip(decodeSplit(ps.family, k.columnFamilyData as ABS)).toMap()
    val q = ps.colqNames.zip(decodeSplit(ps.colq, k.columnQualifierData as ABS)).toMap()
    val vis: Map<Name, Lazy<Any?>> = if (ps.vis == null) mapOf() else mapOf(ps.visName!! to decode(ps.vis, k.columnVisibilityData as ABS))
    val ts: Map<Name, Lazy<Any?>> = if (ps.ts == null) mapOf() else mapOf(ps.tsName!! to decodeTime(ps.ts, k.timestamp))
    val vals = if (v == null) mapOf() else ps.valNames.zip(decodeSplit(ps.pvals, v.get())).toMap()
    map = r+fam+q+vis+ts+vals
  }
  val mapForced = lazy { map.map { (n,v) -> n to v.value }.toMap() } //println("now getting $n");

  override val entries: Set<Map.Entry<String, Any?>> = map.entries.map { object : Map.Entry<String,Any?> {
    override val key: String = it.key
    override val value: Any?
      get() = it.value.value
  } }.toSet()
  override val keys: Set<String> = map.keys
  override val size: Int = map.size
  /** Forces all values. */
  override val values: Collection<Any?> = mapForced.value.values
  override fun containsKey(key: String): Boolean = key in map
  /** Forces all values. */
  override fun containsValue(value: Any?): Boolean = mapForced.value.containsValue(value)
  override fun get(key: String): Any? = map[key]?.value
  override fun isEmpty(): Boolean = map.isEmpty()
  override fun toString(): String = "TupleByKeyValue(map=$map)"

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other == null) return false
    if (other is TupleByKeyValue) return ps == other.ps && k == other.k && v == other.v
    if (other !is Map<*,*>) return false
    return size == other.size && mapForced.value == other // todo - what about default values that may not be present?
  }
  override fun hashCode() = mapForced.value.hashCode()


  companion object {
    fun decodeTime(attr: PAttribute<*>, ts: Long): Lazy<Any?> = lazy { attr.type.decodeLong(ts) }
    fun decode(attr: PAttribute<*>, data: ByteArray): Lazy<Any?> = decodeSplit(listOf(attr), data)[0]
    fun decode(attr: PAttribute<*>, data: ABS): Lazy<Any?> = decodeSplit(listOf(attr), data)[0]
    fun decodeSplit(attrs: List<PAttribute<*>>, data: ByteArray): List<Lazy<Any?>> = decodeSplit(attrs, ABS(data))
    fun decodeSplit(attrs: List<PAttribute<*>>, data: ABS): List<Lazy<Any?>> {
      require(data.isBackedByArray) {"not backed by array: $data"}
      // the prefix of attributes that have a positive width can be directly accessed
      // if the last attribute is -1 variable width and no others are, it can be directly accessed
      // otherwise the first -1 variable width attribute and all subsequent attributes are variable-escape-encoded
      val posWidthAttributePrefix = attrs.takeWhile { it.type.naturalWidth > 0 }
      /** The starting indexes of each attribute, up until the first -1 width.
       * E.g.: the widths 2, 2, 3 yield 0, 2, 4, 7. */
      val posWidthIndexes = posWidthAttributePrefix.fold(listOf(0)) { list, attr -> list + (list.last() + attr.type.naturalWidth) }
      require(data.length() >= posWidthIndexes.last()) {"Insufficient data provided for attributes $attrs: $data"}
      val posWidthAccessors: List<Lazy<Any?>> = posWidthAttributePrefix.mapIndexed { i, attr -> lazy { attr.type.decode(data.backingArray, data.offset() + posWidthIndexes[i], attr.type.naturalWidth) }  }

      return if (posWidthAttributePrefix.size == attrs.size - 1) {
        // only last attribute is variable width case
        val lastAttr = attrs[attrs.size - 1]
        val startIdx = posWidthIndexes.last()
        val lastAccessor = lazy { lastAttr.type.decode(data.backingArray, data.offset() + startIdx, data.length() - startIdx) }
        posWidthAccessors + lastAccessor

      } else if (posWidthAttributePrefix.size < attrs.size - 1) {
        // many attributes after variable width one
        val remainingAttr = attrs.subList(posWidthAttributePrefix.size, attrs.size)
        assert(remainingAttr.size > 1)
        val startIdx = posWidthIndexes.last()

        val GetSplitData = lazy { splitAndUnescape(data.backingArray, data.offset() + startIdx, data.length() - startIdx).apply { require(this.size == remainingAttr.size) }  }

        posWidthAccessors + remainingAttr.mapIndexed { i, lastAttr ->
          lazy {
            val splitData = GetSplitData.value[i]
            assert(splitData.hasArray())
            lastAttr.type.decode(splitData.array(), splitData.arrayOffset() + splitData.position(), splitData.remaining())
          }
        }
      } else {
        // no variable width attributes
        posWidthAccessors
      }
    }



    fun encodeJoin(attr: PAttribute<*>, tuple: Tuple): ByteArray = encodeJoin(listOf(attr), tuple)

    // this will be used on row, family, colq, pvals, etc. of a NameTuple to get the parts to put together into a Key and Value
    // another, higher-level method will cobble these values together into the actual Key and Value
    // (if handed a previous NameTuple, could check to see if the new one has the same key, and then not need to translate again)
    fun encodeJoin(attrs: List<PAttribute<*>>, tuple: Tuple): ByteArray {
      // handled separately: special case if we are handed a NameTuple which is backed by a KeyValue - just use the KeyValue
      val encodedDataList: List<ByteArray> = attrs.map { it.type.encodeUnchecked(tuple[it.name] ?:
          (if (it is PValAttribute<*>) it.default else it.type.naturalDefault)) } // TODO provide default value if tuple[it.name] is not present

      val posWidthAttributePrefix = attrs.takeWhile { it.type.naturalWidth > 0 }
      /** The starting indexes of each attribute, up until the first -1 width (exclusive).
       * E.g.: the widths 2, 2, 3 yield 0, 2, 4, 7. */
      val posWidthIndexes = posWidthAttributePrefix.fold(listOf(0)) { list, attr -> list + (list.last() + attr.type.naturalWidth) }



      return if (posWidthAttributePrefix.size == attrs.size - 1) {
        // only last attribute is variable width case
        val res = ByteArray(posWidthIndexes.last() + encodedDataList.last().size)
        encodedDataList.fold(0) { resStart, encData ->
          System.arraycopy(encData, 0, res, resStart, encData.size)
          resStart + encData.size
        }
        res

      } else if (posWidthAttributePrefix.size < attrs.size - 1) {
        // many attributes after variable width one
        val remainingAttr = attrs.subList(posWidthAttributePrefix.size, attrs.size)
        val remainingDataList = encodedDataList.subList(posWidthAttributePrefix.size, attrs.size)
        assert(remainingAttr.size > 1)

        val joined: ByteBuffer = escapeAndJoin(remainingDataList.map { ByteBuffer.wrap(it) })
        val joinedLen = joined.remaining()

        if (posWidthIndexes.last() == 0 && joined.arrayOffset() == 0 && joined.array().size == joinedLen) {
          // optimization where we use the ByteBuffer's array directly
          joined.array()
        } else {
          val res = ByteArray(posWidthIndexes.last() + joinedLen)
          for (i in 0..posWidthIndexes.size - 2) {
            assert(encodedDataList[i].size == posWidthAttributePrefix[i].type.naturalWidth)
            System.arraycopy(encodedDataList[i], 0, res, posWidthIndexes[i], encodedDataList[i].size)
          }
          //lastAttr.type.decode(splitData.array(), splitData.arrayOffset() + splitData.position(), splitData.remaining())
          System.arraycopy(joined.array(), joined.arrayOffset() + joined.position(), res, posWidthIndexes.last(), joinedLen)
          res
        }
      } else {
        // no variable width attributes
        val posWidthArray = ByteArray(posWidthIndexes.last())
        for (i in 0..posWidthIndexes.size-2) {
          assert(encodedDataList[i].size == posWidthAttributePrefix[i].type.naturalWidth)
          System.arraycopy(encodedDataList[i], 0, posWidthArray, posWidthIndexes[i], encodedDataList[i].size)
        }
        posWidthArray
      }
    }
  } // end companion object
}

/** Pass this to [LoadData] to create a TupleOp. */
class KvToTupleAdapter(val ps: PSchema, private val iter: KeyValueIterator): TupleIterator {
  var top: Tuple? = null

  override fun hasNext(): Boolean = top != null || iter.hasNext()
  override fun next(): Tuple {
    val (k, v) = iter.next()
    top = null
    return TupleByKeyValue(ps, k, v)
  }

  override fun seek(seek: TupleSeekKey) {
    iter.seek(seek.toSeekKey(ps))
    top = null
  }

  override fun peek(): Tuple = if (top != null) top!! else {
    val (k, v) = iter.peek()
    val t = TupleByKeyValue(ps, k, v)
    top = t; t
  }
  override fun deepCopy(env: IteratorEnvironment) = KvToTupleAdapter(ps, iter.deepCopy(env))
}

class TupleToKvAdapter(val ps: PSchema, private val tupleIter: TupleIterator): KeyValueIterator {
  var top: KeyValue? = null

  override fun hasNext(): Boolean = top != null || tupleIter.hasNext()
  override fun next(): KeyValue {
    val tuple = tupleIter.next().toKeyValue(ps)
    top = null
    return tuple
  }

  override fun seek(seek: SeekKey) {
    tupleIter.seek(seek.toTupleSeekKey(ps))
    top = null
  }

  override fun peek(): KeyValue = if (top != null) top!! else {
    val t = tupleIter.peek().toKeyValue(ps)
    top = t; t
  }
  override fun deepCopy(env: IteratorEnvironment) = TupleToKvAdapter(ps, tupleIter.deepCopy(env))
}

/** Note: no no-args constructor. This adapter is not designed as a standalone Accumulo SKVI. */
class KvToSkviAdapter(private val inner: KeyValueIterator): SKVI {
  override fun seek(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {
    logger.debug{"seek: range: $range"}
    @Suppress("UNCHECKED_CAST")
    inner.seek(SeekKey(range, columnFamilies as Collection<ABS>, inclusive))
  }

  override fun deepCopy(env: IteratorEnvironment) = KvToSkviAdapter(inner.deepCopy(env))
  override fun init(source: SKVI, options: Map<String, String>, env: IteratorEnvironment) {}
  override fun hasTop(): Boolean = inner.hasNext()

  override fun next() {
    inner.next()
  }
  override fun getTopKey(): Key = inner.peek().key
  override fun getTopValue(): Value = inner.peek().value

  companion object : Loggable {
    override val logger: Logger = logger<KvToSkviAdapter>()
  }
}

fun mapKvToKvAdapter(mapKv: Iterator<Map.Entry<Key,Value>>): Iterator<KeyValue> {
  return Iterators.transform(mapKv) { KeyValue(it!!) }
}

fun Scanner.asKvIterator(): KeyValueIterator {
  return ScannerKvIterator(this)
}

class ScannerKvIterator(private val scanner: Scanner) : KeyValueIterator {
  var iter: PeekingIterator<Map.Entry<Key, Value>>? = null
  override fun seek(seek: SeekKey) {
    scanner.range = seek.range
    scanner.clearColumns()
    if (!seek.inclusive && seek.families.isNotEmpty())
      throw NOPE("not yet supported: exclusion set of columns")
    seek.families.forEach { scanner.fetchColumn(Text(it.toArray()), EMPTY_TEXT) }
    iter = null
  }

  override fun hasNext(): Boolean {
    if (iter == null) iter = Iterators.peekingIterator(scanner.iterator())
    return iter!!.hasNext()
  }

  override fun next(): KeyValue {
    if (iter == null) iter = Iterators.peekingIterator(scanner.iterator())
    return KeyValue(iter!!.next())
  }

  override fun peek(): KeyValue {
    if (iter == null) iter = Iterators.peekingIterator(scanner.iterator())
    return KeyValue(iter!!.peek()) // this could be optimized by caching top value
  }

  override fun deepCopy(env: IteratorEnvironment) = throw NOPE("no deepCopy")
  companion object {
    val EMPTY_TEXT = Text()
  }
}


