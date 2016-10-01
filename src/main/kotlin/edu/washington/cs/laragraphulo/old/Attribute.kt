package edu.washington.cs.laragraphulo.old

import com.google.common.collect.ImmutableList
import edu.washington.cs.laragraphulo.LexicoderPlus
import edu.washington.cs.laragraphulo.opt.Name
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
    require(width == -1 || width > 0) {"bad width $width"}
  }

  /** Compare Attributes based on their name. */
  object AttributeNameComparator : Comparator<Attribute<*>> {
    override fun compare(o1: Attribute<*>, o2: Attribute<*>): Int {
      return o1.name.compareTo(o2.name)
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
    val attributes: ImmutableList<Name>
) {
  constructor(name: Name, attrs: List<Name>)
  : this(name, ImmutableList.copyOf(attrs))
}


