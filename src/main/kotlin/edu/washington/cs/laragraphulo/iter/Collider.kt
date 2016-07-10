package edu.washington.cs.laragraphulo.iter

import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.SortedKeyValueIterator

/**
 * Handles colliding SKVIs.
 */
interface Collider {
  /**
   * Contract: advance all skvis past the current match.
   */
  fun collide(vararg skvis: SortedKeyValueIterator<Key, Value>): Iterator<Pair<Key,Value>>
}


