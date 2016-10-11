package edu.washington.cs.laragraphulo.util

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.slf4j.Logger
import java.util.*

/**
 * For testing; an iterator that emits entries from a list of hardcoded data.
 *
 * Note: no no-args constructor. Not designed as a standalone Accumulo SKVI.
 */
class MapToSkviAdapter(
    val origMap: SortedMap<Key, Value>
) : SortedKeyValueIterator<Key, Value>
{

  companion object : Loggable {
    override val logger: Logger = logger<MapToSkviAdapter>()
  }

  private lateinit var inner: PeekingIterator<Map.Entry<Key, Value>>
  private lateinit var seekData: SeekData

  override fun init(source: SortedKeyValueIterator<Key, Value>?, options: Map<String, String>, env: IteratorEnvironment) {
    if (source != null)
      logger.warn("MapToSkviAdapter ignores/replaces parent source passed in init(): " + source)
  }

  override fun deepCopy(env: IteratorEnvironment): MapToSkviAdapter {
    val newInstance = MapToSkviAdapter(origMap)
    newInstance.init(null, mapOf(), env)
    return newInstance
  }

  override fun hasTop(): Boolean = inner.hasNext()

  override operator fun next() {
    inner.next()
    // check if we passed the seek r
    findTop()
  }

  private fun Key.matchesFilter(): Boolean =
      seekData.range.contains(this) &&
      !(seekData.inclusive xor seekData.columnFamilies.contains(this.columnFamilyData))
  // incl=T ==> contains
  // incl=F ==> !contains
  // incl=T  contains T F <- xor
  // incl=T !contains F T
  // incl=F  contains F T
  // incl=F !contains T F

  /** check if we passed the seek r or if we need to worry about column families */
  private fun findTop() {
    while (inner.hasNext() && !inner.peek().key.matchesFilter())
      inner.next()
  }


  override fun seek(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {
    seekData = SeekData(range, columnFamilies, inclusive)
    // seek to first entry inside r
    inner = Iterators.peekingIterator(when {
      range.isInfiniteStartKey -> origMap
      range.isStartKeyInclusive -> origMap.tailMap(range.startKey)
      else -> origMap.tailMap(range.startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME))
    }.entries.iterator())
    findTop()
  }

  override fun getTopKey(): Key = inner.peek().key

  override fun getTopValue(): Value = inner.peek().value
}