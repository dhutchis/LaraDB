package edu.washington.cs.laragraphulo.iter

import com.google.common.base.Preconditions
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import edu.washington.cs.laragraphulo.util.SeekData
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.PartialKey
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import java.util.*

/**
 * Operates on a list of SKVIs and a Comparator
 * Aligns the SKVIs and calls a `collide` function when all of them are equal, according to the Comparator.
 *
 * This class WILL advance the skvis when it is constructed, to the first match or until an skvi is exhausted.
 */
class MergeJoin(
    val comparator: SkipKeyComparator,
    val collider: Collider,
    vararg skvisWithSeekData: Pair<SortedKeyValueIterator<Key,Value>, SeekData>
) : PeekingIterator<Pair<Key, Value>>
{

  interface SkipKeyComparator : Comparator<Key> {
    /**
     * Return a Key that the currentKey should seek to in order for this Comparator to indicate
     * that the new key is >= the keyToAdvanceTo.
     */
    fun skipSeekKey(keyToAdvanceTo: Key, currentKey: Key): Key

    class Natural(
        val partialKey: org.apache.accumulo.core.data.PartialKey
    ) : SkipKeyComparator
    {
      override fun skipSeekKey(keyToAdvanceTo: Key, currentKey: Key): Key =
          GraphuloUtil.keyCopy(keyToAdvanceTo, partialKey)

      override fun compare(o1: Key, o2: Key): Int =
          o1.compareTo(o2, partialKey)
    }

    companion object {
      val ROW = Natural(PartialKey.ROW)
      val ROW_COLF_COLQ = Natural(PartialKey.ROW_COLFAM_COLQUAL)
      object ROW_COLQ : SkipKeyComparator {
        /**
         * Returns a key with the same column family as [currentKey]
         * but the row and column qualifier of [keyToAdvanceTo].
         */
        override fun skipSeekKey(keyToAdvanceTo: Key, currentKey: Key): Key =
            Key(keyToAdvanceTo.rowData.toArray(), currentKey.columnFamilyData.toArray(), keyToAdvanceTo.rowData.toArray(), byteArrayOf(), Long.MAX_VALUE)

        /**
         * Two keys are equal if they agree on row and column family.
         */
        override fun compare(o1: Key, o2: Key): Int {
          val t = o2.row
          var result = o1.compareRow(t)
          if (result != 0)
            return result
          o2.getColumnQualifier(t)
          result = o1.compareColumnQualifier(t)
          return result
        }
      }
    }
  }



  val skvis: Array<SortedKeyValueIterator<Key,Value>>
  val seekData: Array<SeekData>

  companion object {
    private val EMPTY_ITER = Iterators.peekingIterator<Pair<Key,Value>>(Collections.emptyIterator())
  }

  private var processed = EMPTY_ITER

  init {
    Preconditions.checkArgument(skvisWithSeekData.size > 0, "Need at least one skvi")
    skvis = Array(skvisWithSeekData.size, { skvisWithSeekData[it].first })
    seekData = Array(skvisWithSeekData.size, { skvisWithSeekData[it].second })
    findTop()
  }

  override fun hasNext(): Boolean = processed.hasNext()

  override fun peek(): Pair<Key, Value> = processed.peek()

  override fun next(): Pair<Key, Value> {
    val n = processed.next()
    findTop()
    return n
  }

  /**
   * Fills [processed] with entries.
   * When this method returns, [processed] is empty iff one of the skvis !hasTop.
   */
  private fun findTop() {
    while (!processed.hasNext() && !skvis.any { !it.hasTop() }) {
      /* Algorithm. Let {i} be the key at position i.
      Set start to 0. Consider position i starting from 1.
      1. If {i-1} == {i}, i++.
      2. If {i-1} < {i}, set start = i and i++.
      3. If {i-1} > {i}, advance {i} to at least {i-1}.
         If !hasTop, return.
         Consider case 1 and 2.
      Stop when i == start. Take i mod skvis.size.
      Process all. Assume the collider advances the skvi to the next row each.
      */
      // note: this is not schema-aware. Can optimize later to take advantage of which attributes are in the predicate.
      var start = 0
      var i = 1 % skvis.size
      while (i != start) {
        val si = skvis[i]
        val sp = skvis[if (i == 0) skvis.size - 1 else (i - 1)]
        do {
          val c = Integer.signum(comparator.compare(sp.topKey, si.topKey))
          when (c) {
            -1 -> start = i
            1 -> {
              if (!advanceSkvi(si, sp.topKey, seekData[i]))
                return
            }
          }
        } while (c == 1)
        i = (i + 1) % skvis.size
      }
      // all skvis match according to the comparator
      processed = Iterators.peekingIterator(collider.collide(*skvis))
    }
  }

  /**
   * Tries to advance [skvi] >= [keyToAdvanceTo] in terms of the [comparator].
   * Calls [SortedKeyValueIterator.next] 10 times and then [SortedKeyValueIterator.seek] if necessary.
   * Uses [seekDatum] to determine how to do seeking; [SkipKeyComparator.skipSeekKey] determines what to seek to.
   * @return true if the [skvi] hasTop() and is advanced >= [keyToAdvanceTo] in terms of the comparator.
   *         false if it cannot be done.
   */
  private fun advanceSkvi(skvi: SortedKeyValueIterator<Key, Value>, keyToAdvanceTo: Key, seekDatum: SeekData): Boolean {
    val MAX_NEXT_ATTEMPT = 10
    var cnt = 0
    while (cnt < MAX_NEXT_ATTEMPT && skvi.hasTop() && comparator.compare(keyToAdvanceTo, skvi.topKey) > 0) {
      skvi.next()
      cnt++
    }
    if (skvi.hasTop() && comparator.compare(keyToAdvanceTo, skvi.topKey) > 0) {
      // set target range to beginning of pk
      val seekKey = comparator.skipSeekKey(keyToAdvanceTo, skvi.topKey)
      val skipToRange = Range(seekKey, true, null, false).clip(seekDatum.range, true) ?: // row we want to get to does not exist, and it is out of our range
          return false
      skvi.seek(skipToRange, seekDatum.columnFamilies, seekDatum.inclusive)
    }
    return skvi.hasTop()
  }

  override fun remove() {
    throw UnsupportedOperationException("not implemented")
  }
}




