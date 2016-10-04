package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import edu.washington.cs.laragraphulo.opt.raco.RacoType
import org.apache.accumulo.core.data.ArrayByteSequence
import java.util.*
import java.util.regex.Pattern


/**
 * An attribute name.
 */
typealias Name = String

/** An alias for the ArrayByteSequence. This is the primary data holder. */
typealias ABS = ArrayByteSequence

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
typealias Default = ByteArray

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
    get() = keyNames.subList(0,dapLen)
  /** local access path; the second portion of [keyNames] */
  val lap: List<Name>
    get() = keyNames.subList(dapLen, keyNames.size)
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
interface SortedSchema : KeySchema {
  /** An int such that all [KeySchema.keyNames] whose index is less than sortedUpto are sorted.
   * 0 means nothing is sorted. Valid up to and including [KeySchema.keyNames].size. */
  val sortedUpto: Int
  /** Whether there are multiple tuples with the same key attribute values. */
  val duplicates: Boolean
}
interface APSortedSchema : SortedSchema, APSchema
interface EncodingSchema {
  val encodings: Map<Name, Type<*>>
}
interface ReducingSchema {
  val reducers: Map<Name, (List<FullValue>) -> FullValue>
}

//interface UberSchema {
//  val allAtts: List<Name>
//  val types: List<Type<*>>
////  val reducers:
//  val lapLen: Int
//
//  override val dap: List<String>
//    get() = keyNames.subList(0,dapLen)
//  override val lap: List<String>
//    get() = super.lap
//  override val dapRange: IntRange
//    get() = super.dapRange
//  override val lapRange: IntRange
//    get() = super.lapRange
//}

fun defaultWidths(types: List<Type<*>>): List<Width> {
  types.map { it.naturalWidth }
}


const val __DAP__ = "__DAP__"
const val __LAP__ = "__LAP__"

class ImmutableUberSchema(
    allNames: List<Name>,
    dapLen: Int,
    lapLen: Int,
    /** An int such that all [keyNames] whose index is less than sortedUpto are sorted.
     * 0 means nothing is sorted. Valid up to and including [keyNames].size. */
    val sortedUpto: Int,
    types: List<Type<*>>,
    /** Only for keys (dap and lap) */
    widths: List<Width> = types.map { it.naturalWidth },
    /** Only for values */
    defaults: List<Default> = types.map { it.naturalDefaultEncoded },
    // not sure if this should be here
    family: ABS = EMPTY
) {
  val all: List<Name> = ImmutableList.copyOf(allNames)
  val dap: List<Name> = all.subList(0,dapLen)
  val lap: List<Name> = all.subList(dapLen,dapLen+lapLen)
  val cap: List<Name> = all.subList(dapLen+lapLen,all.size)
  val types: List<Type<*>> = ImmutableList.copyOf(types)
  val widths: List<Width> = ImmutableList.copyOf(widths)
  val defaults: List<Default> = ImmutableList.copyOf(defaults)
  val fam: ByteArray = Arrays.copyOfRange(family.backingArray, family.offset(), family.offset()+family.length())

  fun toRacoScheme(): List<Pair<Name,Type<*>>> {
    return all.zip(types).foldIndexed(ImmutableList.builder<Pair<Name, Type<*>>>()) { i, builder, pair -> when (i) {
      dap.size -> builder.add(__DAP__ to Type.STRING).apply { if (lap.size == dap.size) add(__LAP__ to Type.STRING) }
      lap.size -> builder.add(__LAP__ to Type.STRING)
      else -> builder
    }.add(pair) }.build()
//    val list: ArrayList<Pair<Name, Type<*>>> = all.zip(types).mapTo(ArrayList<Pair<Name, Type<*>>>(all.size + 2))
//    list.add(dap.size+lap.size, __LAP__ to Type.STRING)
//    list.add(dap.size, __DAP__ to Type.STRING)
//    return list
  }

  companion object {
    fun fromRacoScheme(scheme: List<Pair<Name, Type<*>>>): ImmutableUberSchema {
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

      return ImmutableUberSchema(
          namesNoDapNoLap, daplen, laplen,
          sortedUpto = 0, types = typesNoDapNoLap
      )
    }
  }

  override fun toString(): String{
    return "USchema(dap=$dap, lap=$lap, cap=$cap)"
    // sortedUpto=$sortedUpto, all=$all, dap=$dap, lap=$lap, cap=$cap, widths=$widths, defaults=$defaults, fam=${Arrays.toString(fam)}
  }


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
) : ImmutableAccessPath(dap, lap), APSortedSchema {
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



