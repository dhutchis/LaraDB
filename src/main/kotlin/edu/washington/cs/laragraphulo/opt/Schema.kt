package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import org.apache.accumulo.core.data.ArrayByteSequence
import java.io.Serializable
import java.util.regex.Pattern

/*
These interfaces are capability interfaces.
They do not enforce any error checking. That is left to implementing classes.
 */

/** An ordered list of attribute names. */
interface NameSchema : Serializable {
  /** Attribute names in order. Whether these contain keys, values, both, or something else depends on context.
   * When it contains keys and values, the keys come first (first dapNames, then lapNames) and values come second. */
  val allNames: List<Name>
}

/** An ordered list of key attribute names. */
interface KeySchema : Serializable {
  /** An ordered list of key attribute names. Does not include value attributes. */
  val keyNames: List<Name>
}

/** A refinement of [KeySchema] that divides [keyNames] into [dapNames] and [lapNames]. */
interface APKeySchema : KeySchema {
  /** distributed access path; the first portion of [keyNames] */
  val dapNames: List<Name>
  /** local access path; the second portion of [keyNames] */
  val lapNames: List<Name>

  override val keyNames: List<Name>
    get() = dapNames + lapNames

  /** Shortcut property for indexing into [keyNames] */
  val dapRange: IntRange
    get() = 0..dapNames.size-1
  /** Shortcut property for indexing into [keyNames] */
  val lapRange: IntRange
    get() = dapNames.size..dapNames.size+lapNames.size-1
}

/** A refinement of [NameSchema] that divides [allNames] into [keyNames] and [valNames]. */
interface KVSchema : NameSchema {
  val keyNames: List<Name>
  val valNames: List<Name>

  override val allNames: List<String>
    get() = keyNames + valNames

  val keyRange: IntRange
    get() = 0..keyNames.size-1
  val valRange: IntRange
    get() = keyNames.size..allNames.size-1
}

/** The [allNames] here are fully divided into [dapNames], [lapNames], [valNames]. */
interface APSchema : APKeySchema, KVSchema {
  override val keyNames: List<String>
    get() = super.keyNames
}

/** A refinement of [KeySchema] that specifies how large a prefix of the [keyNames] are sorted. */
interface SortedKeySchema : KeySchema {
  /** An int such that all [KeySchema.keyNames] whose index is less than sortedUpto are sorted.
   * 0 means nothing is sorted. Valid up to and including [KeySchema.keyNames].size. */
  val sortedUpto: Int
}

/** [Type] of each attribute. */
interface TypeSchema : Serializable {
  val types: List<Type<*>>
}

//interface DefaultSchema : NameSchema {
//  /**
//   * Interpretation for missing values.
//   */
//  val defaults: List<Default>
//}

/** [Width] of each attribute. */
interface WidthSchema : Serializable {
  /**
   * A width for each name
   *
   * `>= 0` means fixed width.
   * `-1` means variable width.
   *
   * The last attribute in the dapNames may be variable width.
   * Todo: Support other variable-width encoding.
   */
  val widths: List<Width>
}



const val __DAP__ = "__DAP__"
const val __LAP__ = "__LAP__"

//class ImmutableUberSchema(
//    allNames: List<Name>,
//    dapLen: Int,
//    lapLen: Int,
//    /** An int such that all [dapNames] and [lapNames] whose index is less than sortedUpto are sorted.
//     * 0 means nothing is sorted. Valid up to and including [dapNames].size + [lapNames].size. */
//    override val sortedUpto: Int,
//    types: List<Type<*>>,
//    /** Only for keys (dapNames and lapNames); not used for value attributes */
//    widths: List<Width> = types.map { it.naturalWidth },
//    /** Only for values */
//    defaults: List<Default> = types.map { it.naturalDefaultEncoded }
//    // not sure if this should be here
////    family: ABS = EMPTY
//)  {
//  override val allNames: List<Name> = ImmutableList.copyOf(allNames)
//  override val keyNames: List<Name> = this.allNames.subList(0,dapLen+lapLen)
//  override val dapNames: List<Name> = this.keyNames.subList(0,dapLen)
//  override val lapNames: List<Name> = this.keyNames.subList(dapLen,dapLen+lapLen)
//  override val valNames: List<Name> = this.allNames.subList(dapLen+lapLen, this.allNames.size)
//  override val types: List<Type<*>> = ImmutableList.copyOf(types)
//  /** Only for keys (dapNames and lapNames); not used for value attributes */
//  override val widths: List<Width> = ImmutableList.copyOf(widths)
//  /** Only for values */
//  val defaults: List<Default> = ImmutableList.copyOf(defaults)
////  val fam: ByteArray = Arrays.copyOfRange(family.backingArray, family.offset(), family.offset()+family.length())
//
//  fun toRacoScheme(): List<Pair<Name,Type<*>>> {
//    return allNames.zip(types).foldIndexed(ImmutableList.builder<Pair<Name, Type<*>>>()) { i, builder, pair -> when (i) {
//      dapNames.size -> builder.add(__DAP__ to Type.STRING).apply { if (lapNames.size == dapNames.size) add(__LAP__ to Type.STRING) }
//      lapNames.size -> builder.add(__LAP__ to Type.STRING)
//      else -> builder
//    }.add(pair) }.build()
////    val list: ArrayList<Pair<Name, Type<*>>> = all.zip(types).mapTo(ArrayList<Pair<Name, Type<*>>>(all.size + 2))
////    list.add(dapNames.size+lapNames.size, __LAP__ to Type.STRING)
////    list.add(dapNames.size, __DAP__ to Type.STRING)
////    return list
//  }
//
//  override fun toString(): String{
//    return "USchema(dapNames=${dapNames}, lapNames=${lapNames}, cap=${valNames})"
//    // sortedUpto=$sortedUpto, all=$all, dapNames=$dapNames, lapNames=$lapNames, cap=$cap, widths=$widths, defaults=$defaults, fam=${Arrays.toString(fam)}
//  }
//}




fun fromRacoScheme(scheme: List<Pair<Name, Type<*>>>): AccessPath {
  val (names, types) = scheme.unzip()
  val dapidx = names.indexOf(__DAP__)
  val daplen: Int
  val namesNoDap: List<Name>
  val typesNoDap: List<Type<*>>
  if (dapidx == -1) {
    daplen = names.size
    namesNoDap = names
    typesNoDap = types
  } else {
    daplen = dapidx
    namesNoDap = names.subList(0,dapidx) + if (dapidx+1 < names.size) names.subList(dapidx+1,names.size) else emptyList()
    typesNoDap = types.subList(0,dapidx) + if (dapidx+1 < names.size) types.subList(dapidx+1,names.size) else emptyList()
  }
  val lapidx = namesNoDap.indexOf(__LAP__)
  val laplen: Int
  val namesNoDapNoLap: List<Name>
  val typesNoDapNoLap: List<Type<*>>
  if (lapidx == -1) {
    laplen = names.size - daplen
    namesNoDapNoLap = namesNoDap
    typesNoDapNoLap = typesNoDap
  } else {
    require(lapidx >= daplen) { "$__DAP__ appears after $__LAP__ in $names" }
    laplen = lapidx - daplen
    namesNoDapNoLap = namesNoDap.subList(0,lapidx) + if (lapidx+1 < namesNoDap.size) namesNoDap.subList(lapidx+1,namesNoDap.size) else emptyList()
    typesNoDapNoLap = typesNoDap.subList(0,lapidx) + if (lapidx+1 < typesNoDap.size) typesNoDap.subList(lapidx+1,typesNoDap.size) else emptyList()
  }
  // this could be simplified; I converted the code

//  return ImmutableUberSchema(
//      namesNoDapNoLap, daplen, laplen,
//      sortedUpto = 0, types = typesNoDapNoLap
//  )
  return AccessPath.of(namesNoDapNoLap, daplen, laplen, typesNoDapNoLap)
}






data class NameTypeWidth(
    val name: Name,
    val type: Type<*>,
    val width: Width = type.naturalWidth
) : Serializable {
  override fun toString(): String = "<$name@$type@$width>"
}


/** An ordered list of attribute names.
 * Context determines whether these are a list of just the key attributes, just the value attributes,
 * both, or something else. */
sealed class Schema(
    allNames: List<Name>,
    types: List<Type<*>>, // = allNames.map { Type.UNKNOWN },
    /** Only for keys (dapNames and lapNames); meaningless for value attributes */
    widths: List<Width> // = types.map { it.naturalWidth }
//    /** Only for values; meaningless for key attributes (dapNames and lapNames) */
//    defaults: List<ByteArray> = types.map { it.naturalDefaultEncoded }
) : NameSchema, TypeSchema, WidthSchema { // List<Name> by keyNames,
  override val allNames: List<Name> = ImmutableList.copyOf(allNames)
  final override val types: List<Type<*>> = ImmutableList.copyOf(types)
  final override val widths: List<Width> = ImmutableList.copyOf(widths)
//  val defaults: List<Int> = ImmutableList.copyOf(allNames)

  init {
    // check for duplicate names
    val set = allNames.toSet()
    require(set.size == allNames.size) {"There is a duplicate attribute name in: $allNames"}
    // check for invalid names
    set.forEach { checkName(it) }
    // types and widths and defaults
    require(types.size == allNames.size) {"Number of widths != number of attributes: $widths, $allNames"}
    require(widths.size == allNames.size) {"Number of widths != number of attributes: $widths, $allNames"}
  }

  companion object {
    /** The regular expression specifying what names are valid.  */
    const val VALID_NAME_REGEX = "^[a-zA-Z_]\\w*$"
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

    fun of(
        allNames: List<Name> = listOf(),
        types: List<Type<*>> = allNames.map { Type.UNKNOWN },
        widths: List<Width> = types.map { it.naturalWidth }
    ): Schema = SchemaImpl(allNames, types, widths)
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
//  open fun getSubAttributes(idxs: IntArray): ImmutableKeySchema =
//      ImmutableKeySchemaImpl(names.slice(idxs.asIterable()))


//  operator fun contains(name: Name): Boolean = name in nameToIndex

  // consider overriding +, -

  val allZipTypeWidth: List<NameTypeWidth> by lazy {
    val b = ImmutableList.builder<NameTypeWidth>()
    assert(allNames.size == types.size && types.size == widths.size) {
      "There are a different number of allNames, types, widths: $allNames, $types, $widths"}
    for (i in 0..allNames.size-1) {
      b.add(NameTypeWidth(allNames[i], types[i], widths[i]))
    }
    b.build()
  }

  override fun toString() = "Schema$allZipTypeWidth"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Schema

    if (allNames != other.allNames) return false
    if (types != other.types) return false
    if (widths != other.widths) return false

    return true
  }
  override fun hashCode(): Int{
    var result = allNames.hashCode()
    result = 31 * result + types.hashCode()
    result = 31 * result + widths.hashCode()
    return result
  }


  private class SchemaImpl(
      allNames: List<Name>,
      types: List<Type<*>>,
      widths: List<Width>
  ) : Schema(allNames, types, widths)
}



sealed class KVAccessPath(
    keyNames: List<Name>,
    valNames: List<Name>,
    types: List<Type<*>>,
    widths: List<Width>
) : KVSchema, Schema(ImmutableList.builder<Name>().addAll(keyNames).addAll(valNames).build(), types, widths) {
  final override val allNames: List<String> = super<Schema>.allNames
  override val keyNames: List<Name> = ImmutableList.copyOf(keyNames)
  final override val valNames: List<Name> = ImmutableList.copyOf(valNames)

//  init {
//    require(cap.sumBy { it.attributes.count() } == valNames.size) {
//      "one of the attributes was mentioned twice in two separate column families $cap"
//    }
//  }

  companion object {
    fun of(keyNames: List<Name> = listOf(),
           valNames: List<Name> = listOf(),
           types: List<Type<*>> = (keyNames+valNames).map { Type.UNKNOWN },
           widths: List<Width> = types.map { it.naturalWidth }
    ): KVAccessPath = KVAccessPathImpl(keyNames, valNames, types, widths)

    fun of(allNames: List<Name>,
           keylen: Int,
           types: List<Type<*>> = allNames.map { Type.UNKNOWN },
           widths: List<Width> = types.map { it.naturalWidth }
    ): KVAccessPath {
      val an = ImmutableList.copyOf(allNames)
      val keyNames = an.subList(0,keylen)
      val valNames = an.subList(keylen,an.size)
      return KVAccessPathImpl(keyNames, valNames, types, widths)
    }
  }

  private class KVAccessPathImpl(
      keyNames: List<Name>,
      valNames: List<Name>,
      types: List<Type<*>>,
      widths: List<Width>
  ) : KVAccessPath(keyNames, valNames, types, widths)

  val keyZipTypeWidth: List<NameTypeWidth>
    get() = allZipTypeWidth.subList(0,keyNames.size)
  val valZipTypeWidth: List<NameTypeWidth>
    get() = allZipTypeWidth.subList(keyNames.size,allNames.size)

  override fun toString() = "AP(key=$keyZipTypeWidth; val=$valZipTypeWidth)"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as KVAccessPath
    if (types != other.types) return false
    if (widths != other.widths) return false

    if (keyNames != other.keyNames) return false
    if (valNames != other.valNames) return false

    return true
  }
  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + types.hashCode()
    result = 31 * result + widths.hashCode()
    result = 31 * result + keyNames.hashCode()
    result = 31 * result + valNames.hashCode()
    return result
  }


}




// I will consider the cap---column access path---later.

sealed class AccessPath(
    dapNames: List<Name>,
    lapNames: List<Name>,
    valNames: List<Name>,
    types: List<Type<*>>,
    widths: List<Width>
) : APSchema, KVAccessPath(ImmutableList.builder<Name>().addAll(dapNames).addAll(lapNames).build(), valNames, types, widths) {
  final override val keyNames: List<String> = super<KVAccessPath>.keyNames
  final override val dapNames: List<Name> = ImmutableList.copyOf(dapNames)
  final override val lapNames: List<Name> = ImmutableList.copyOf(lapNames)

//  init {
//    require(cap.sumBy { it.attributes.count() } == valNames.size) {
//      "one of the attributes was mentioned twice in two separate column families $cap"
//    }
//  }

  companion object {
    fun of(dapNames: List<Name> = listOf(),
           lapNames: List<Name> = listOf(),
           valNames: List<Name> = listOf(),
           types: List<Type<*>> = (dapNames+lapNames+valNames).map { Type.UNKNOWN },
           widths: List<Width> = types.map { it.naturalWidth }
    ): AccessPath = AccessPathImpl(dapNames, lapNames, valNames, types, widths)

    fun of(allNames: List<Name>,
           daplen: Int,
           laplen: Int,
           types: List<Type<*>> = allNames.map { Type.UNKNOWN },
           widths: List<Width> = types.map { it.naturalWidth }
    ): AccessPath {
      val an = ImmutableList.copyOf(allNames)
      val dapNames = an.subList(0,daplen)
      val lapNames = an.subList(daplen,daplen+laplen)
      val valNames = an.subList(daplen+laplen,an.size)
      return AccessPathImpl(dapNames, lapNames, valNames, types, widths)
    }
  }

  private class AccessPathImpl(
      dapNames: List<Name>,
      lapNames: List<Name>,
      valNames: List<Name>,
      types: List<Type<*>>,
      widths: List<Width>
  ) : AccessPath(dapNames, lapNames, valNames, types, widths)

  val dapZipTypeWidth: List<NameTypeWidth>
    get() = keyZipTypeWidth.subList(0,dapNames.size)
  val lapZipTypeWidth: List<NameTypeWidth>
    get() = keyZipTypeWidth.subList(dapNames.size,dapNames.size+lapNames.size)

  fun withSortedUpto(sortedUpto: Int) = SortedAccessPath.of(dapNames, lapNames, valNames, sortedUpto, types, widths)

  override fun toString() = "AP(dap=$dapZipTypeWidth; lap=$lapZipTypeWidth; val=$valZipTypeWidth)"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
//    if (!super.equals(other)) return false

    other as AccessPath
    if (types != other.types) return false
    if (widths != other.widths) return false

    if (dapNames != other.dapNames) return false
    if (lapNames != other.lapNames) return false
    if (valNames != other.valNames) return false

    return true
  }
  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + types.hashCode()
    result = 31 * result + widths.hashCode()
    result = 31 * result + dapNames.hashCode()
    result = 31 * result + lapNames.hashCode()
    result = 31 * result + valNames.hashCode()
    return result
  }
}


sealed class SortedAccessPath(
    dapNames: List<Name>,
    lapNames: List<Name>,
    valNames: List<Name>,
    final override val sortedUpto: Int,
    types: List<Type<*>>,
    widths: List<Width>
) : SortedKeySchema, AccessPath(dapNames, lapNames, valNames, types, widths) {
  init {
    Preconditions.checkPositionIndex(sortedUpto, dapNames.size+lapNames.size, "sortedUpto is an int such that all keyNames $keyNames " +
        "whose index is less than sortedUpto are sorted. 0 means nothing is sorted. Valid up to and including ${dapNames.size+lapNames.size}. Given: $sortedUpto")
  }

  companion object {
    fun of( dapNames: List<Name> = listOf(),
            lapNames: List<Name> = listOf(),
            valNames: List<Name> = listOf(),
            sortedUpto: Int = -1,
            types: List<Type<*>> = (dapNames+lapNames+valNames).map { Type.UNKNOWN },
            widths: List<Width> = types.map { it.naturalWidth }
    ): SortedAccessPath = SortedAccessPathImpl(dapNames, lapNames, valNames,
        if (sortedUpto == -1) dapNames.size+lapNames.size else sortedUpto,
        types, widths)

    fun of(allNames: List<Name>,
           daplen: Int,
           laplen: Int,
           sortedUpto: Int = -1,
           types: List<Type<*>> = allNames.map { Type.UNKNOWN },
           widths: List<Width> = types.map { it.naturalWidth }
    ): SortedAccessPath {
      val an = ImmutableList.copyOf(allNames)
      val dapNames = an.subList(0,daplen)
      val lapNames = an.subList(daplen,daplen+laplen)
      val valNames = an.subList(daplen+laplen,an.size)
      return SortedAccessPathImpl(dapNames, lapNames, valNames,
          if (sortedUpto == -1) dapNames.size+lapNames.size else sortedUpto,
          types, widths)
    }
  }

  private class SortedAccessPathImpl(
      dapNames: List<Name>,
      lapNames: List<Name>,
      valNames: List<Name>,
      sortedUpto: Int,
      types: List<Type<*>>,
      widths: List<Width>
  ) : SortedAccessPath(dapNames, lapNames, valNames, sortedUpto, types, widths)

  override fun toString() = "AP(dap$dapZipTypeWidth; lap=$lapZipTypeWidth; val=$valZipTypeWidth; sort=$sortedUpto)"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    if (!super.equals(other)) return false

    other as SortedAccessPath

    if (sortedUpto != other.sortedUpto) return false

    return true
  }

  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + sortedUpto
    return result
  }


}



