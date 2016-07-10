package edu.washington.cs.laragraphulo.util

import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import java.io.IOException
import java.util.*

/**
 * [PeekingIterator] wrapper around a [SortedKeyValueIterator].
 * Assumes the skvi is already init'ed and seek'ed.
 */
class SkviToIteratorAdapter(
    private val inner: SortedKeyValueIterator<Key, Value>
) : PeekingIterator<Pair<Key, Value>> {

  private var res: Pair<Key,Value>? = null

  override fun peek(): Pair<Key, Value> {
    if (res == null) {
      if (!inner.hasTop())
        throw NoSuchElementException()
      res = Pair(inner.topKey, inner.topValue)
    }
    return res!!
  }

  override fun remove() {
    throw UnsupportedOperationException()
  }

  override fun hasNext(): Boolean {
    return inner.hasTop()
  }

  override fun next(): Pair<Key, Value> {
    val result = peek()
    res = null
    try {
      inner.next()
      return result
    } catch (ex: IOException) {
      throw NoSuchElementException("IOException from the inner skvi: $ex")
    }
  }
}