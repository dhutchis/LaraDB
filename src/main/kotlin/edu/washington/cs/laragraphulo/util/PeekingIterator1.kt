package edu.washington.cs.laragraphulo.util

import java.util.*

/**
 * Iterator that caches an entry. See [org.apache.accumulo.core.util.PeekingIterator].
 * Make sure you call [hasNext] before calling [peek] or [next] to avoid exceptions.
 */
class PeekingIterator1<E> : Iterator<E> {
  val source: Iterator<E>
  var top: E? = null

  constructor(source: Iterator<E>?) {
    this.source = source ?: Collections.emptyIterator<E>()
    top = if (!this.source.hasNext()) null else this.source.next()
  }

  /** Create a PeekingIterator1 with given starting element.  */
  constructor(source: Iterator<E>?, top: E) {
    this.source = source ?: Collections.emptyIterator<E>()
    this.top = top
  }

  /** Create a PeekingIterator1 with a single element.  */
  constructor(top: E) {
    this.source = Collections.emptyIterator<E>()
    this.top = top
  }

  fun peek(): E {
    if (top == null)
      throw NoSuchElementException()
    return top!!
  }

  override fun next(): E {
    if (top == null)
      throw NoSuchElementException()
    val lastPeeked = top!!
    top = if (source.hasNext()) source.next() else null
    return lastPeeked
  }

  override fun hasNext(): Boolean {
    return top != null
  }

  companion object {
    private val EMPTY_ITERATOR = PeekingIterator1<Any>(null)

    @Suppress("CAST_NEVER_SUCCEEDS")
    fun <T> emptyIterator(): PeekingIterator1<T> {
      return EMPTY_ITERATOR as PeekingIterator1<T>
    }
  }
}
