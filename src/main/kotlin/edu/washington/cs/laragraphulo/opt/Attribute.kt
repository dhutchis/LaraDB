package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSortedMap
import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.client.lexicoder.Lexicoder
import java.io.Serializable
import java.util.*


/**
 * @param[T] Data type of this attribute's values.
 */
data class Attribute<T>(
    val name: Name,
    /**
     * `> 0` means fixed width.
     * `-1` means variable width.
     */
    val width: Int,
    /**
     * A method for encoding to and decoding from a byte[].
     */
    val encoder: LexicoderPlus<T>,
    /**
     * Interpretation for missing values.
     * Maybe include it in ByteArray format too?
     * null means no default value.
     */
    val default: T
) : Serializable {
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


//class AttributeMap private constructor(
//    map: NavigableMap<String, Attribute<*>>
//): NavigableMap<String, Attribute<*>> by map {
//  constructor() : this(TreeMap<String, Attribute<*>>())
//}

data class ColumnFamily internal constructor(
    val name: Name,
    val attributes: ImmutableList<Attribute<*>>
) {
  constructor(name:Name, attrs: Collection<Attribute<*>>)
  : this(name, ImmutableList.copyOf(attrs))
}


