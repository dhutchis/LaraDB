package edu.washington.cs.laragraphulo

import org.apache.accumulo.core.client.lexicoder.Lexicoder
import org.apache.accumulo.core.data.ArrayByteSequence
import java.io.Serializable

/**
 * An interface for encoding but not decoding.
 */
interface Encode<in T> : Serializable {
  fun encode(v: T): ByteArray
}

/**
 * Exposes a method to decode part of a byte array.
 */
interface LexicoderPlus<T> : Lexicoder<T>, Encode<T>, Serializable {
  fun decode(b: ByteArray, off: Int, len: Int): T
  override fun decode(b: ByteArray): T = decode(b, 0, b.size)
  override fun encode(v: T): ByteArray
  fun decode(abs: ArrayByteSequence): T = decode(abs.backingArray, abs.offset(), abs.length())
}