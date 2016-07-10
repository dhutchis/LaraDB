package edu.washington.cs.laragraphulo.util

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.log4j.LogManager
import java.util.*

/**
 * For testing; an iterator that emits entries from a list of hardcoded data.
 */
class MapToSkviAdapter(
    val origMap: SortedMap<Key, Value>
) : SortedKeyValueIterator<Key, Value>
{
  private val log = LogManager.getLogger(MapToSkviAdapter::class.java)

  companion object {
    private val EMPTY_ITER = Iterators.peekingIterator<Map.Entry<Key,Value>>(Iterators.emptyIterator())
  }

  private lateinit var inner: PeekingIterator<Map.Entry<Key, Value>>
  private lateinit var seekRng: Range

  override fun init(source: SortedKeyValueIterator<Key, Value>?, options: Map<String, String>, env: IteratorEnvironment) {
    if (source != null)
      log.warn("MapToSkviAdapter ignores/replaces parent source passed in init(): " + source)
  }

  override fun deepCopy(env: IteratorEnvironment): MapToSkviAdapter {
    val newInstance = MapToSkviAdapter(origMap)
    newInstance.init(null, mapOf(), env)
    return newInstance
  }

  override fun hasTop(): Boolean = inner.hasNext()

  override operator fun next() {
    inner.next()
    // check if we passed the seek range
    if (inner.hasNext() && !seekRng.contains(inner.peek().key))
      inner = EMPTY_ITER
  }

  override fun seek(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {
    // TODO adapt to handle columnFamilies
    seekRng = range
    // seek to first entry inside range
    inner = Iterators.peekingIterator(when {
      range.isInfiniteStartKey -> origMap
      range.isStartKeyInclusive -> origMap.tailMap(range.startKey)
      else -> origMap.tailMap(range.startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME))
    }.entries.iterator())
    // check if we passed the seek range
    if (inner.hasNext() && !seekRng.contains(inner.peek().key))
      inner = EMPTY_ITER
  }

  override fun getTopKey(): Key = inner.peek().key

  override fun getTopValue(): Value = inner.peek().value
}