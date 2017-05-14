package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.api.TupleOp.*
import edu.washington.cs.laragraphulo.encoding.escapeAndJoin
import edu.washington.cs.laragraphulo.encoding.splitAndUnescape
import edu.washington.cs.laragraphulo.opt.EMPTY_B
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import java.nio.ByteBuffer
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.atomic.AtomicLong


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


// todo: extend transform to pass along a value from parents - add list of parent's values to method signature
fun TupleOp.instantiateLoadOnce(tableMap: Map<Table, Iterator<NameTuple>>): TupleOp = this.transform {
  when (it) {
    is Load -> {
      require(it.table in tableMap) { "Attempt to lower a TupleOp stack but no SKVI given for ${it.table}" }
      LoadOnce(it.resultSchema, tableMap[it.table]!!)
    }
    else -> it
  }
}
fun TupleOp.instantiateLoad(tableMap: Map<Table, Iterable<NameTuple>>): TupleOp = this.transform {
  when (it) {
    is Load -> {
      require(it.table in tableMap) { "Attempt to lower a TupleOp stack but no SKVI given for ${it.table}" }
      LoadData(it.resultSchema, tableMap[it.table]!!)
    }
    else -> it
  }
}

fun TupleOp.getBaseTables(): Set<Table> = this.fold(setOf<Table>(), { a, b -> a + b }) { when(it) {
  is Load -> setOf(it.table)
  else -> setOf()
} }

fun TupleOp.getWrittenTables(): Set<Table> = this.fold(setOf<Table>(), { a, b -> a + b }) { when(it) {
  is Store -> setOf(it.table)
  else -> setOf()
} }

private var lastTime = 0L
private fun getTime(): Long {
  var t = System.currentTimeMillis()
  if (t == lastTime) t++ // no duplicates please (need more control for multi-threaded; fine for single-thread)
  lastTime = t
  return t
}
private val dateFormat: DateFormat = SimpleDateFormat("'temp_'yyyyMMdd_HHmmssSSS")
fun genName(): String = dateFormat.format(Date(getTime()))

// todo - gen date string, Store, DeepCopy

/** Create a list of TupleOps in topological order, from the top-most Loads down.
 * Each one contains TupleOps until a Sort (pipeline-breaker).
 * At that point, a Store to a temporary table is added and the next TupleOps read from a Load to that table.
 * If a multi-parent TupleOp stems from the same Load, add a DeepCopy on the p2. */
fun TupleOp.splitPipeline(): List<TupleOp> {
  // maybe keep more information like what tables we need to create, but we could get that information from the Store operators too
  // every pipeline ends in a Store
  val pipelines: MutableList<Store> = LinkedList()

  val remaining = this.transform { when(it) {
    is MergeJoin -> {
      val p1t = it.p1.getBaseTables() // todo - pass along the base tables during the transform; this is less efficient
      val p2t = it.p2.getBaseTables()
      if (p1t.disjoint(p2t)) it else it.copy(p2 = DeepCopy(it.p2))
    }
    is MergeUnion0.MergeUnion -> if (it.p1.getBaseTables().disjoint(it.p2.getBaseTables())) it else it.copy(p2 = DeepCopy(it.p2))
    is Sort -> {
      val tempTable = genName()
      val pipeline = Store(it, tempTable)
      pipelines += pipeline
      Load(tempTable, it.resultSchema)
    }
    is Store -> {
      // treat like Sort - end the pipeline and start a new one
      pipelines += it
      Load(it.table, it.resultSchema)
    }
    else -> it
  } }

  if (remaining !is Load) {
    // switch this to a logger for whatever class this is
    println("WARN: Dead code elimination: $remaining")
  }

  return pipelines
}

// Key, Value -> NameTuple
// PType of each attribute
// that has all the information we need to decode
// map of Attribute in Schema to PType that implements that attribute
// + where the PType is stored: row, column family, column qualifier, timestamp, value
// row --
// colf --
// ensure all types are covered
// we want to create functions that obtain the data for each attribute:
// tuple["t"] --> ptype.decode( key[row_idx] )

interface PAttribute<T> : Attribute<T> {
  override val type: PType<T>
  override fun component2(): PType<T> = type
  override fun withNewName(name: Name): PAttribute<T> = PAttribute(name, type)
  fun asPValAttribute() = PValAttribute(name, type, type.naturalDefault)

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
  override fun asPValAttribute() = this

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
class PhysicalSchema(
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


  fun encodeToKeyValue(t: NameTuple): KeyValue {
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

    other as PhysicalSchema

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
    return "PhysicalSchema(row=$row, family=$family, colq=$colq, vis=$vis, ts=$ts, pvals=$pvals)"
  }


}

class TupleByKeyValue(ps: PhysicalSchema, val k: Key, val v: Value?): Map<String,Any?> {
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
  val mapForced = lazy { map.map { (n,v) -> n to v.value }.toMap() }

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



    fun encodeJoin(attr: PAttribute<*>, tuple: NameTuple): ByteArray = encodeJoin(listOf(attr), tuple)

    // this will be used on row, family, colq, pvals, etc. of a NameTuple to get the parts to put together into a Key and Value
    // another, higher-level method will cobble these values together into the actual Key and Value
    // (if handed a previous NameTuple, could check to see if the new one has the same key, and then not need to translate again)
    fun encodeJoin(attrs: List<PAttribute<*>>, tuple: NameTuple): ByteArray {
      // handled separately: special case if we are handed a NameTuple which is backed by a KeyValue - just use the KeyValue
      val encodedDataList: List<ByteArray> = attrs.map { it.type.encodeUnchecked(tuple[it.name] ?:
          (if (it is PValAttribute<*>) it.default else it.type.naturalDefault)) } // TODO provide default value if tuple[it.name] is not present

      val posWidthAttributePrefix = attrs.takeWhile { it.type.naturalWidth > 0 }
      /** The starting indexes of each attribute, up until the first -1 width.
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
class KvToTupleAdapter(val ps: PhysicalSchema, val iter: Iterator<KeyValue>): Iterator<NameTuple> {
//  private val skviIter = SkviToIteratorAdapter(skvi)

  override fun hasNext(): Boolean = iter.hasNext()

  override fun next(): NameTuple {
    val (k, v) = iter.next()
    return TupleByKeyValue(ps, k, v)
  }
}

class TupleToKvAdapter(val ps: PhysicalSchema, private val tupleIter: Iterator<NameTuple>): Iterator<KeyValue> {
  override fun hasNext(): Boolean = tupleIter.hasNext()
  override fun next(): KeyValue {
    val tuple = tupleIter.next()
    if (tuple is TupleByKeyValue && tuple.v != null)
      return KeyValue(tuple.k,tuple.v)
    return ps.encodeToKeyValue(tuple)
  }
}

