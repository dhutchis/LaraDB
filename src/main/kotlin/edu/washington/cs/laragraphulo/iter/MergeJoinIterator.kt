package edu.washington.cs.laragraphulo.iter

import com.google.common.base.Preconditions
import com.google.common.collect.Iterators
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import java.util.*

/**
 * Operates on a list of SKVIs and a Comparator
 * Aligns the SKVIs and calls a `collide` function when all of them are equal, according to the Comparator.
 */
class MergeJoinIterator(
    val comparator: Comparator<Key>,
    val collider: Collider,
    vararg skvis: SortedKeyValueIterator<Key,Value>)
: SortedKeyValueIterator<Key,Value> {
  init {
    Preconditions.checkArgument(skvis.size > 0, "Need at least one skvi")
  }

  val skvis = skvis
  var start = 0

  var processed = Iterators.peekingIterator<Pair<Key,Value>>(Collections.emptyIterator())


  override fun seek(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
    for (skvi in skvis) {
      skvi.seek(range, columnFamilies, inclusive)
    }
    findTop()
  }

  private fun findTop() {
    if (!skvis[0].hasTop())
      return
    var i = 1
    while (i != start) {
      val ip = if (i == 0) skvis.size-1 else (i-1) % skvis.size
      if (!skvis[i].hasTop())
        return
      val c = comparator.compare(skvis[ip].topKey, skvis[i].topKey)
      when (Integer.signum(c)) {
        // todo
      }
    }
  }

  override fun next() {
    throw UnsupportedOperationException()
  }

  override fun getTopKey(): Key? {
    throw UnsupportedOperationException()
  }

  override fun hasTop(): Boolean {
    throw UnsupportedOperationException()
  }

  override fun getTopValue(): Value? {
    throw UnsupportedOperationException()
  }

  override fun init(source: SortedKeyValueIterator<Key, Value>?, options: MutableMap<String, String>?, env: IteratorEnvironment?) {
    throw UnsupportedOperationException()
  }

  override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key, Value>? {
    throw UnsupportedOperationException()
  }

  //  data class Args(
//      val comparator: Comparator<Key>,
//      val collider: Collider,
//      val skvis: SortedKeyValueIterator<Key,Value>)
//  )
}




