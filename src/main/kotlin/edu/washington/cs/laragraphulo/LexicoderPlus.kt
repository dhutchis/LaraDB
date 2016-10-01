package edu.washington.cs.laragraphulo

import org.apache.accumulo.core.client.lexicoder.Lexicoder

/**
 * An interface for encoding but not decoding.
 */
interface Encode<in T> {
  fun encode(v: T): ByteArray
}

/**
 * Exposes a method to decode part of a byte array.
 */
interface LexicoderPlus<T> : Lexicoder<T>, Encode<T> {
  fun decode(b: ByteArray, offset: Int, len: Int): T
  override fun decode(b: ByteArray): T = decode(b, 0, b.size)
  override fun encode(v: T): ByteArray
}