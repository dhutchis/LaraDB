package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.data.ArrayByteSequence
import java.util.regex.Pattern


/**
 * `>= 0` means fixed width.
 * `-1` means variable width.
 */
typealias Width = Int

/** Attribute/column position */
typealias Position = Int

/**
 * Interpretation for missing values.
 */
typealias Default = ArrayByteSequence

/*
These interfaces are capability interfaces.
They do not enforce any error checking. That is left to implementing classes.
 */

//interface NameSchema {
//  val names: List<Name>
//}
interface WidthSchema {
  /**
   * A width for each key attribute.
   *
   * `>= 0` means fixed width.
   * `-1` means variable width.
   *
   * The last attribute in the dap may be variable width.
   * Todo: Support other variable-width encoding.
   */
  val widths: List<Width>
//    require(widths.size == names.size) {"widths and names sizes differ: $widths ; $names"}
//    require(widths.all { it == -1 || it >= 0 }) {"There is a bad width: $widths"}
}
interface DefaultSchema {
  /**
   * Interpretation for missing values.
   */
  val defaults: List<Default>
//    require(defaults.size == names.size) {"defaults and names sizes differ: $defaults ; $names"}
}
interface KeySchema {
  val keyNames: List<Name>
//  val valNames: List<Name>
}
interface APSchema : KeySchema {
  /** Length of the distributed access path. Partitions the key values into the dap and lap. */
  val dapLen: Int
  /** distributed access path; the first portion of [keyNames] */
  val dap: List<Name>
    get() = lazy { keyNames.subList(0,dapLen) }.value
  /** local access path; the second portion of [keyNames] */
  val lap: List<Name>
    get() = lazy { keyNames.subList(dapLen, keyNames.size) }.value
  val dapRange: IntRange
    get() = 0..dapLen-1
  val lapRange: IntRange
    get() = dapLen..dapLen+lap.size-1
//  val lapOff: Int
//      get() = dapLen
//  val lapLen: Int
//      get() = keyNames.size - dapLen
//  /**
//   * column access path; defines the [valNames]
//   *
//   * A list of the attribute groups. Each group is potentially stored in a different file.
//   * The ordering of attributes within groups is lexicographic.
//   */
//  val cap: List<ColumnFamily>
}
interface SortedSchema : APSchema {
  /** An int such that all [KeySchema.keyNames] whose index is less than sortedUpto are sorted.
   * 0 means nothing is sorted. Valid up to and including [KeySchema.keyNames].size. */
  val sortedUpto: Int
  /** Whether there are multiple tuples with the same key attribute values. */
  val duplicates: Boolean
}
interface EncodingSchema {
  val encodings: Map<Name, Type<*>>
}
interface ReducingSchema {
  val reducers: Map<Name, (List<FullValue>) -> FullValue>
}





sealed class ImmutableKeySchema(
    keyNames: List<Name>
): KeySchema { // List<Name> by keyNames,
  final override val keyNames: List<Name> = ImmutableList.copyOf(keyNames)
  init {
    // check for duplicate names
    val set = this.keyNames.toSet()
    require(set.size == this.keyNames.size) {"There is a duplicate attribute name: ${this.keyNames}"}
    // check for invalid names
    set.forEach { checkName(it) }
  }

  companion object {
    /** The regular expression specifying what names are valid.  */
    val VALID_NAME_REGEX = "^[a-zA-Z_]\\w*$"
    /** The regular expression matcher for [.VALID_NAME_REGEX].  */
    private val VALID_NAME_PATTERN = Pattern.compile(VALID_NAME_REGEX)
    /**
     * Validate a potential column name for use in a Schema. Valid names are given by [.VALID_NAME_REGEX].
     * @param name the candidate column name.
     * @return passes through the argument name
     * @throws IllegalArgumentException if the name does not match the regex [.VALID_NAME_REGEX].
     */
    fun checkName(name: String): String {
      require(VALID_NAME_PATTERN.matcher(name).matches()) {"supplied column name $name does not match the valid name regex $VALID_NAME_REGEX"}
      return name
    }

    fun of(attrs: List<Name>): ImmutableKeySchema = ImmutableKeySchemaImpl(attrs)
  }

//  /**
//   * Return true if the two schema are "compatible": they have the same size and column types; column names are ignored.
//   */
//  fun compatible(a2: Attributes): Boolean {
//    return names.size == a2.names.size &&
//        names.zip(a2.names).all { it.first.encoder == it.second.encoder }
//  }


  // todo: test this to see if it improves performance
////  @Transient
//  private val nameToIndex: Map<Name, Int> by lazy {
//    ImmutableMap.copyOf(names.mapIndexed { i, attribute -> attribute to i }.toMap())
//  }

//  /**
//   * Find the index of the column with a given name.
//   * @throws NoSuchElementException if no column with a matching name is found.
//   */
//  fun columnNameToIndex(name: String): Int =
//      this.indexOf(name)
//      nameToIndex[name] ?: throw NoSuchElementException("No column named $name found; names are ${nameToIndex.keys}")

//  /**
//   * Return a subset of the current schema.
//   * @param idxs indices to be selected.
//   */
//  open fun getSubAttribtues(idxs: IntArray): ImmutableKeySchema =
//      ImmutableKeySchemaImpl(names.slice(idxs.asIterable()))


//  operator fun contains(name: Name): Boolean = name in nameToIndex

  // consider overriding +, -

  override fun toString(): String = "ImmutableKeySchema$keyNames"
  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as ImmutableKeySchema

    if (keyNames != other.keyNames) return false

    return true
  }

  override fun hashCode(): Int{
    return keyNames.hashCode()
  }


  private class ImmutableKeySchemaImpl(attrs: List<Name>)
  : ImmutableKeySchema(ImmutableList.copyOf(attrs))
}


sealed class ImmutableAccessPath(
    dap: List<Name>,
    lap: List<Name>
) : ImmutableKeySchema(
    ImmutableList.builder<Name>().addAll(dap).addAll(lap).build()
), APSchema {
  final override val dapLen = dap.size
  final override val dap = super.dap
  final override val lap = super.lap
//  init {
//    require(cap.sumBy { it.attributes.count() } == valNames.size) {
//      "one of the attributes was mentioned twice in two separate column families $cap"
//    }
//  }

  companion object {
    fun of( dap: List<Name>,
            lap: List<Name>): ImmutableAccessPath = ImmutableAccessPathImpl(dap,lap)
  }

  private class ImmutableAccessPathImpl(
      dap: List<Name>,
      lap: List<Name>
  ) : ImmutableAccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap))

  override fun toString(): String = "ImmutableAccessPath(dap=$dap, lap=$lap)"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as ImmutableAccessPath

    if (dap != other.dap) return false
    if (lap != other.lap) return false

    return true
  }

  override fun hashCode(): Int{
    var result = dap.hashCode()
    result = 31 * result + lap.hashCode()
    return result
  }


}


// need to subclass ImmutableAccessPath because this tells us how to interpret each part of the Key/Value
sealed class ImmutableBagAccessPath(
    /** @see [APSchema.dap] */
    dap: List<Name>,
    /** @see [APSchema.lap] */
    lap: List<Name>,
    final override val sortedUpto: Int,
    final override val duplicates: Boolean
) : ImmutableAccessPath(dap, lap), SortedSchema {
  init {
    Preconditions.checkPositionIndex(sortedUpto, dap.size+lap.size, "sortedUpto is an int such that all keyNames $keyNames " +
        "whose index is less than sortedUpto are sorted. 0 means nothing is sorted. Valid up to and including ${dap.size+lap.size}. Given: $sortedUpto")
  }

  companion object {
    fun of( dap: List<Name>,
            lap: List<Name>,
            sortedUpto: Int = -1,
            duplicates: Boolean = false): ImmutableBagAccessPath = ImmutableBagAccessPathImpl(dap, lap,
        if (sortedUpto == -1) dap.size+lap.size else sortedUpto, duplicates)
  }

  private class ImmutableBagAccessPathImpl(
      dap: List<Name>,
      lap: List<Name>,
      sortedUpto: Int,
      duplicates: Boolean
  ) : ImmutableBagAccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap), sortedUpto, duplicates)

  override fun toString(): String{
    val s = StringBuilder("ImmutableBagAccessPath(dap=$dap, lap=$lap")
    if (sortedUpto != dap.size+lap.size)
      s.append(", sortedUpto=$sortedUpto")
    if (duplicates)
      s.append(", dups")
    s.append(")")
    return s.toString()
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    if (!super.equals(other)) return false

    other as ImmutableBagAccessPath

    if (sortedUpto != other.sortedUpto) return false
    if (duplicates != other.duplicates) return false

    return true
  }

  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + sortedUpto
    result = 31 * result + duplicates.hashCode()
    return result
  }


}



