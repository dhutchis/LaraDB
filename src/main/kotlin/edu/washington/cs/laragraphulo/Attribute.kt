package edu.washington.cs.laragraphulo

import com.google.common.base.Preconditions
import org.apache.accumulo.core.client.lexicoder.Lexicoder


/**
 * @param[T] Data type of this attribute's values.
 */
data class
Attribute<T>(val name: String,
             /**
              * `> 0` means fixed width.
              * `-1` means variable width.
              */
             val width: Int,
             /**
              * A method for encoding to and decoding from a byte[].
              */
             val encoder: Lexicoder<T>) {
  init {
    Preconditions.checkArgument(width == -1 || width > 0, "bad width")
  }

  /** Used as a fill-in for Attributes valued as a ByteArray. */
  class IdentityEncoder : Lexicoder<ByteArray> {
    override fun encode(v: ByteArray): ByteArray = v
    override fun decode(b: ByteArray): ByteArray = b
  }
}
