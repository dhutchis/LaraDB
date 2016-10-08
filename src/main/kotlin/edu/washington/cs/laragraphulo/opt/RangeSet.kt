package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableSortedSet
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.Range

import java.util.*

/**
 * Pass in a target set of ranges, such as { [b,g], (j,m], [q,+inf) }. Infinite r by default.
 * Iterate over the ones that overlap with a given seek r, such as (-inf,k].
 * Results is iteration returning [b,g], (j,k].
 */
class RangeSet {

  private var targetRanges: SortedSet<Range> = INF_RANGE_SET

  /** Set the target ranges that we will iterator over, before applying a "seek r mask".
   * Merges overlapping ranges together.  Infinite r by default.  */
  fun setTargetRanges(ranges: Collection<Range>) {
    targetRanges = TreeSet(Range.mergeOverlapping(TreeSet(ranges)))
  }

  fun size(): Int {
    return targetRanges.size
  }

  /** Iterate over target ranges in order, masked by seekRange.
   * Only iterates over target ranges that intersect the seekRange.  */
  fun iteratorWithRangeMask(seekRange: Range): PeekingIterator<Range> {
    if (seekRange.isInfiniteStartKey && seekRange.isInfiniteStopKey)
      return Iterators.peekingIterator(targetRanges.iterator())
    else if (seekRange.isInfiniteStartKey)
      return RangeSetIter(targetRanges.iterator(), seekRange)
    else {
      // find first r whose end key >= the start key of seekRange
      val pi = getFirstRangeStarting(seekRange, targetRanges)
      return RangeSetIter(pi, seekRange)
    }
  }

  private class RangeSetIter : PeekingIterator<Range> {
    private val seekRange: Range
    private var source: Iterator<Range>
    private var top: Range? = null

    internal constructor(sourceIter: Iterator<Range>, seekRange: Range) {
      this.seekRange = seekRange
      source = sourceIter
    }

    private fun findTop() {
      if (top == null && source.hasNext()) {
        top = seekRange.clip(source.next(), true)
        if (top == null) // stop early
          source = Collections.emptyIterator()
      }
    }

    @Deprecated("unsupported", level = DeprecationLevel.ERROR, replaceWith = ReplaceWith("assert(false) {\"remove is not supported\"}"))
    override fun remove() {
      throw UnsupportedOperationException("unsupported")
    }

    override fun peek(): Range {
      findTop()
      return top ?: throw NoSuchElementException()
    }

    override fun hasNext(): Boolean {
      findTop()
      return top != null
    }

    override operator fun next(): Range {
      findTop()
      val t = top
      top = null
      return t ?: throw NoSuchElementException()
    }
  }

  companion object {
    private val INF_RANGE_SET = ImmutableSortedSet.of(Range())

    /**
     * Advance to the first subset r whose end key >= the seek start key.
     */
    fun getFirstRangeStarting(seekRange: Range, rowRanges: SortedSet<Range>): PeekingIterator<Range> {
      val iter = Iterators.peekingIterator(rowRanges.iterator())
      val seekRangeStart = seekRange.startKey
      if (seekRangeStart != null)
        while (iter.hasNext() && !iter.peek().isInfiniteStopKey()
            && (iter.peek().getEndKey().compareTo(seekRangeStart) < 0 || iter.peek().getEndKey().equals(seekRangeStart) && !seekRange.isEndKeyInclusive))
          iter.next()
      return iter
    }
  }

}
