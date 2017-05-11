package edu.washington.cs.laragraphulo.util

import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.api.KeyValue
import edu.washington.cs.laragraphulo.api.KeyValueIterator
import edu.washington.cs.laragraphulo.api.SeekKey
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import java.io.IOException
import java.util.*

/**
 * [PeekingIterator] wrapper around a [SortedKeyValueIterator].
 * Assumes the skvi is already init'ed and seek'ed.
 */
class SkviToIteratorAdapter(
    private val inner: SortedKeyValueIterator<Key, Value>
) : KeyValueIterator {

  private var res: KeyValue? = null

  override fun peek(): KeyValue {
    val r0 = res
    return if (r0 == null) {
      if (!inner.hasTop())
        throw NoSuchElementException()
      val r = KeyValue(inner.topKey, inner.topValue)
      res = r
      r
    } else r0
  }

  @Deprecated("unsupported", ReplaceWith("assert(false) {\"remove is not supported\"}"), DeprecationLevel.ERROR)
  override fun remove() = throw UnsupportedOperationException("remove is not supported")

  override fun hasNext(): Boolean = inner.hasTop()

  override fun next(): KeyValue {
    val result = peek()
    res = null
    try {
      inner.next()
      return result
    } catch (ex: IOException) {
      throw NoSuchElementException("IOException from the inner skvi: $ex")
    }
  }

  override fun seek(seek: SeekKey) = inner.seek(seek.range, seek.families, seek.inclusive)

  override fun deepCopy(env: IteratorEnvironment) = SkviToIteratorAdapter(inner.deepCopy(env))
}