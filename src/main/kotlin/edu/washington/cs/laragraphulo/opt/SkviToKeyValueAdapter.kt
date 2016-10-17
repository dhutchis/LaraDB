package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.slf4j.Logger
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

  override fun deepCopy(env: IteratorEnvironment): SkviToKeyValueAdapter {
    return SkviToKeyValueAdapter(inner.deepCopy(env))
  }
}

/** Note: no no-args constructor. This adapter is not designed as a standalone Accumulo SKVI. */
class KeyValueToSkviAdapter(
    private val inner: KeyValueIterator
): SortedKeyValueIterator<Key,Value> {
  override fun seek(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {
    logger.debug{"seek: range: $range"}
    @Suppress("UNCHECKED_CAST")
    inner.seek(SeekKey(GraphuloUtil.rangeToGuavaRange(range), columnFamilies as Collection<ArrayByteSequence>, inclusive))
  }

  override fun deepCopy(env: IteratorEnvironment): SortedKeyValueIterator<Key, Value> {
    return KeyValueToSkviAdapter(inner.deepCopy(env))
  }

  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment) {

  }

  override fun hasTop(): Boolean = inner.hasNext()

  override fun next() {
    inner.next()
  }

  override fun getTopKey(): Key = inner.peek().key

  override fun getTopValue(): Value = inner.peek().value

  companion object : Loggable {
    override val logger: Logger = logger<OpCSVScan>()
  }
}

