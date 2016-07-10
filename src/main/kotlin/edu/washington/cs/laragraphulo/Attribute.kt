package edu.washington.cs.laragraphulo

import com.google.common.base.Preconditions
import org.apache.accumulo.core.client.lexicoder.Lexicoder
import java.io.Serializable
import java.util.*


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
             val encoder: LexicoderPlus<T>) : Serializable {
  init {
    Preconditions.checkArgument(width == -1 || width > 0, "bad width")
  }

  /** Compare Attributes based on their name. */
  object AttributeNameComparator : Comparator<Attribute<*>> {
    override fun compare(o1: Attribute<*>?, o2: Attribute<*>?): Int {
      return o1!!.name.compareTo(o2!!.name)
    }
  }


  /** Used as a fill-in for Attributes valued as a ByteArray. */
  object IdentityEncoder : Lexicoder<ByteArray> {
    override fun encode(v: ByteArray): ByteArray = v
    override fun decode(b: ByteArray): ByteArray = b
  }
}
