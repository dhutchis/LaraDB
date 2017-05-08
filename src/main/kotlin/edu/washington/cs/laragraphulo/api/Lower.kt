package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.api.NameTupleOp.*
import edu.washington.cs.laragraphulo.api.NameTupleOp.MergeUnion0.*
import edu.washington.cs.laragraphulo.encoding.escapeAndJoin
import edu.washington.cs.laragraphulo.encoding.splitAndUnescape
import edu.washington.cs.laragraphulo.opt.ABS
import edu.washington.cs.laragraphulo.opt.SKVI
import java.nio.ByteBuffer
import java.util.*


fun NameTupleOp.getBaseTables(): Set<Table> = when(this) {
  is Load -> setOf(this.table)
  is Ext -> this.parent.getBaseTables()
  is Empty -> setOf()
  is MergeUnion0 -> this.p1.getBaseTables() + this.p2.getBaseTables()
  is Rename -> this.p.getBaseTables()
  is Sort -> this.p.getBaseTables()
  is MergeJoin -> this.p1.getBaseTables() + this.p2.getBaseTables()
  is ScanFromData -> setOf()
}

fun NameTupleOp.lower(tableMap: Map<Table, SKVI>): NameTupleOp = when(this) {
  is Ext -> Ext(this.parent.lower(tableMap), this.extFun)
  is Empty -> this
  is MergeUnion -> MergeUnion(this.p1.lower(tableMap), this.p2.lower(tableMap), this.plusFuns)
  is MergeAgg -> MergeAgg(this.p1.lower(tableMap), keysKept, plusFuns)
  is Rename -> Rename(this.p.lower(tableMap), renameMap)
  is Sort -> Sort(p.lower(tableMap), newSort)
  is MergeJoin -> MergeJoin(this.p1.lower(tableMap), this.p2.lower(tableMap), timesFuns)
  is ScanFromData -> this
  is Load -> {
    require(this.table in tableMap) {"Attempt to lower a NameTupleOp stack but no SKVI given for $table"}
    // wrap around SKVI to convert Key/Value entries to a map. Need a Schema
    TODO()
  }
}

fun NameTupleOp.getBaseTables0(): Set<Table> = this.fold(setOf<Table>(), { a, b -> a + b}) { when(it) {
  is Load -> setOf(it.table)
  else -> setOf()
} }

// Key, Value -> NameTuple
// PType of each attribute
// that has all the information we need to decode
// map of Attribute in NameSchema to PType that implements that attribute
// + where the PType is stored: row, column family, column qualifier, timestamp, value
// row --
// colf --
// ensure all types are covered
// we want to create functions that obtain the data for each attribute:
// tuple["t"] --> ptype.decode( key[row_idx] )

open class PAttribute<T>(
    name: Name,
    override val type: PType<T>
) : Attribute<T>(name, type) {
  override fun withNewName(n: Name) = PAttribute(name, type)
  override fun toString(): String {
    return "P"+super.toString()
  }
}


// diamond problem: solved with interfaces: this should also inherit PAttribute<T>
class PValAttribute<T>(
    name: Name,
    type: PType<T>,
    val default: T
) : PAttribute<T>(name, type) {
  override fun withNewName(n: Name) = PValAttribute(name, type, default)
  override fun toString(): String {
    return "P"+super.toString()
  }
}


// I am restricting this to one key-value pair per tuple.
// A more relaxed implementation would put the value attributes separately, or even group them based on vertical partitioning.
// See FullValue and KeyValueToTuple for the more advanced multiple key-values per tuple ideas.
data class PhysicalSchema(
    val row: List<PAttribute<*>>,
    val family: List<PAttribute<*>>,
    val colq: List<PAttribute<*>>,
    val vis: PAttribute<*>?,
    val ts: PAttribute<*>?,
    val vals: List<PValAttribute<*>>
) {
  init {
    val rowNames = row.map(PAttribute<*>::name)
    val familyNames = family.map(PAttribute<*>::name)
    val colqNames = colq.map(PAttribute<*>::name)
    val visName = vis?.name ?: listOf<Name>()
    val tsName = ts?.name ?: listOf<Name>()
    val valNames = vals.map(PValAttribute<*>::name)
    val allNames = rowNames + familyNames + colqNames + visName + tsName + valNames
    require(allNames.size == allNames.toSet().size) {"one of the attributes' names is duplicated; $this"}
  }


  companion object {
    fun decodeSplit(attrs: List<PAttribute<*>>, data: ABS): List<() -> Any?> {
      require(data.isBackedByArray) {"not backed by array: $data"}
      // the prefix of attributes that have a positive width can be directly accessed
      // if the last attribute is -1 variable width and no others are, it can be directly accessed
      // otherwise the first -1 variable width attribute and all subsequent attributes are variable-escape-encoded
      val posWidthAttributePrefix = attrs.takeWhile { it.type.naturalWidth > 0 }
      /** The starting indexes of each attribute, up until the first -1 width.
       * E.g.: the widths 2, 2, 3 yield 0, 2, 4, 7. */
      val posWidthIndexes = posWidthAttributePrefix.fold(listOf(0)) { list, attr -> list + (list.last() + attr.type.naturalWidth) }
      require(data.length() >= posWidthIndexes.last()) {"Insufficient data provided for attributes $attrs: $data"}
      val posWidthAccessors: List<() -> Any?> = posWidthAttributePrefix.mapIndexed { i, attr -> { attr.type.decode(data.backingArray, data.offset() + posWidthIndexes[i], attr.type.naturalWidth) }  }

      return if (posWidthAttributePrefix.size == attrs.size - 1) {
        // only last attribute is variable width case
        val lastAttr = attrs[attrs.size - 1]
        val startIdx = posWidthIndexes.last()
        val lastAccessor = { lastAttr.type.decode(data.backingArray, data.offset() + startIdx, data.length() - startIdx) }
        posWidthAccessors + lastAccessor

      } else if (posWidthAttributePrefix.size < attrs.size - 1) {
        // many attributes after variable width one
        val remainingAttr = attrs.subList(posWidthAttributePrefix.size, attrs.size)
        assert(remainingAttr.size > 1)
        val startIdx = posWidthIndexes.last()

        val GetSplitData = lazy { splitAndUnescape(data.backingArray, data.offset() + startIdx, data.length() - startIdx).apply { require(this.size == remainingAttr.size) }  }

        posWidthAccessors + remainingAttr.mapIndexed { i, lastAttr ->
          {
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

    // next step is to create a NameTuple whose values are backed by a key-value's data

    // this will be used on row, family, colq, vals, etc. of a NameTuple to get the parts to put together into a Key and Value
    // another, higher-level method will cobble these values together into the actual Key and Value
    // (if handed a previous NameTuple, could check to see if the new one has the same key, and then not need to translate again)
    fun encodeJoin(attrs: List<PAttribute<*>>, tuple: NameTuple): ByteArray {
      // TODO special case if we are handed a NameTuple which is backed by a KeyValue - just use the KeyValue
      val encodedDataList: List<ByteArray> = attrs.map { it.type.encodeUnchecked(tuple[it.name]) } // TODO provide default value if tuple[it.name] is not present

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
        posWidthIndexes.last()

        val joined: ByteBuffer = escapeAndJoin(remainingDataList.map { ByteBuffer.wrap(it) })
        val joinedLen = joined.remaining()

        val res = ByteArray(posWidthIndexes.last() + joinedLen)
        for (i in 0..posWidthIndexes.size-2) {
          assert(encodedDataList[i].size == posWidthAttributePrefix[i].type.naturalWidth)
          System.arraycopy(encodedDataList[i], 0, res, posWidthIndexes[i], encodedDataList[i].size)
        }
        System.arraycopy(joined, 0, res, posWidthIndexes.last(), joinedLen)

        res
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
  }

}

