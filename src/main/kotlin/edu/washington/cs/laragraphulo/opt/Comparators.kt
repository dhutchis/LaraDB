package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.PartialKey
import org.apache.hadoop.io.WritableComparator
import java.util.*
import kotlin.comparisons.compareBy


object TupleComparatorByKeys : Comparator<Tuple> {
  /** Comparison on all attributes */
  override fun compare(o1: Tuple, o2: Tuple): Int {
    val i1 = o1.keys.iterator()
    val i2 = o2.keys.iterator()
    while (i1.hasNext() && i2.hasNext()) {
      val c = i1.next().compareTo(i2.next())
      if (c != 0) return c
    }
    return o1.keys.size - o2.keys.size
  }
}

// todo - consider caching these objects
data class TupleComparatorByKeyPrefix(val size: Int) : Comparator<Tuple> {
  override fun compare(o1: Tuple, o2: Tuple): Int {
    require(o1.keys.size >= size && o2.keys.size >= size) {"Tuples do not have enough keys to compare by $size keys: $o1; $o2"}
    for (i in 0..size-1) {
      val c = o1.keys[i].compareTo(o2.keys[i])
      if (c != 0) return c
    }
    return 0
  }
}

/** Empty iterators sort last. */
data class TupleIteratorComparatorByPrefix(val size: Int) : Comparator<PeekingIterator<out Tuple>> {
  val tcomp = TupleComparatorByKeyPrefix(size)
  override fun compare(o1: PeekingIterator<out Tuple>, o2: PeekingIterator<out Tuple>): Int =
      when {
        o1 === o2 -> 0
        !o1.hasNext() -> 1
        !o2.hasNext() -> -1
        else -> tcomp.compare(o1.peek(), o2.peek())
      }
}



val KeyValueComparatorByKey: Comparator<KeyValue> = compareBy() { it.key }


data class KeyValueComparatorToQualifierPrefix(val cqPrefix: Int): Comparator<Key> {
  override fun compare(o1: Key, o2: Key): Int {
    val c = o1.compareTo(o2, PartialKey.ROW_COLFAM)
    if (c != 0) return c

    val cq1 = o1.columnQualifierData
    val cq2 = o2.columnQualifierData
    return WritableComparator.compareBytes(
        cq1.backingArray, cq1.offset(), Math.min(cq1.length(), cqPrefix),
        cq2.backingArray, cq2.offset(), Math.min(cq2.length(), cqPrefix))
  }


}

//object KeyValueComparatorByKey : Comparator<KeyValue> {
//
//}