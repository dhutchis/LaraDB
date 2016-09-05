package edu.washington.cs.laragraphulo.opt

import java.util.*

/**
 * Emits Cartesian product of provided iterators, passing a list of Tuples to a multiply function.
 * The first iterator can be streamed through. Subsequent iterators must be reset-able.
 * Pass switched as true if the two are switched.
 *
 * Proactively performs the first multiplication upon instantiation.
 *
 * Guarantee: once [hasNext] is false, [firstIter] will be exhausted
 */
class CartesianIterator(
    private val firstIter: Iterator<Tuple>,
    private val remainingLists: List<Iterable<Tuple>>,
    private val multiplyOp: MultiplyOp
) : Iterator<Tuple> {
  private val iters = Array<Iterator<Tuple>>(remainingLists.size+1) { if (it == 0) firstIter else remainingLists[it-1].iterator() }
  private val curTuples: Array<Tuple>
  private var multIter: Iterator<Tuple> = Collections.emptyIterator()

  init {
    if (iters.any { !it.hasNext() }) {
      while (firstIter.hasNext()) firstIter.next()
      curTuples = arrayOf()
    } else {
      curTuples = Array<Tuple>(iters.size) { iters[it].next() }
      prepNext(false)
    }
  }

  /*
  1. scan left until we find a position where hasNext() is true. If all are false then terminate.
  2. advance that iterator at the position and fill in curTuples
  3. reset all iterators to the right and fill in curTuples
   */

  override fun hasNext(): Boolean {
    return multIter.hasNext()
  }

  override fun next(): Tuple {
    val ret = multIter.next()
    if (!multIter.hasNext())
      prepNext(true)
    return ret
  }

  private fun prepNext(doNext: Boolean) {
    if (doNext) {
      var idx = curTuples.size - 1
      while (idx >= 0 && !iters[idx].hasNext()) idx--
      if (idx == -1) return
      // iters[idx].hasNext() == true
      curTuples[idx] = iters[idx].next()
      while (++idx < curTuples.size) {
        iters[idx] = remainingLists[idx-1].iterator()
        curTuples[idx] = iters[idx].next()
      }
    }
    multIter = multiplyOp.invoke(curTuples)
  }
}
