package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import org.apache.accumulo.core.data.ArrayByteSequence

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

fun <T> Iterator<T>.peeking() = Iterators.peekingIterator(this)
