package edu.washington.cs.laragraphulo.util

import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.opt.KeyValue
import edu.washington.cs.laragraphulo.opt.KeyValueIterator
import edu.washington.cs.laragraphulo.opt.SeekKey
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
class SkviToKeyValueAdapter(
    private val inner: SortedKeyValueIterator<Key, Value>
) : KeyValueIterator {

  private var res: KeyValue? = null

  override fun peek(): KeyValue {
    if (res == null) {
      if (!inner.hasTop())
        throw NoSuchElementException()
      res = KeyValue(inner.topKey, inner.topValue)
    }
    return res!!
  }

  override fun hasNext(): Boolean {
    return inner.hasTop()
  }

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

  override fun seek(seek: SeekKey) {
    inner.seek(GraphuloUtil.guavaRangeToRange(seek.range), seek.families, seek.inclusive)
  }

  override fun serializeState(): ByteArray {
    throw UnsupportedOperationException("not implemented")
  }

  override fun deepCopy(env: IteratorEnvironment): KeyValueIterator {
    throw UnsupportedOperationException("not implemented")
  }
}

class KeyValueToSkviAdapter(

): SKVI

