package edu.washington.cs.laragraphulo

import org.apache.accumulo.core.client.lexicoder.Lexicoder

/**
 * Exposes a method to decode part of a byte array.
 */
interface LexicoderPlus<T> : Lexicoder<T> {
  fun decode(b: ByteArray, offset: Int, len: Int): T

  override fun decode(b: ByteArray): T = decode(b, 0, b.size)
}