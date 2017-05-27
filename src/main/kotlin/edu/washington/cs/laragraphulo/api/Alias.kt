package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.ArrayByteSequence

typealias Tuple = Map<Name,*>
fun Tuple.toKeyValue(ps: PSchema): KeyValue
    = if (this is TupleByKeyValue && this.v != null) KeyValue(this.k,this.v)
      else ps.encodeToKeyValue(this)

typealias NOPE = UnsupportedOperationException
typealias SKVI = org.apache.accumulo.core.iterators.SortedKeyValueIterator<org.apache.accumulo.core.data.Key,org.apache.accumulo.core.data.Value>


/** An attribute name. */
typealias Name = String
/** An alias for the ArrayByteSequence, which is a wrapper around a ByteArray, possibly at a data offset and restricted length. */
typealias ABS = ArrayByteSequence
/**
 * `>= 0` means fixed width.
 * `-1` means variable width.
 */
typealias Width = Int
/** A table name */
typealias Table = String

fun <T> Iterator<T>.peeking(): PeekingIterator<T> = Iterators.peekingIterator(this)
