package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSortedMap
import old.Schema
import java.util.*
import java.util.regex.Pattern


open class RelationSchema protected constructor(
    val attributes: ImmutableList<Attribute<*>>
) {
  init {
    // check for duplicate names
    val names = attributes.map { it.name }.toSet()
    Preconditions.checkArgument(names.size == attributes.size, "There is a duplicate attribute name: ", names)
    // check for invalid names
    names.forEach { checkName(it) }
  }

  constructor(attrs: Collection<Attribute<*>>)
  : this(ImmutableList.copyOf(attrs))

  companion object {
    /** The regular expression specifying what names are valid.  */
    val VALID_NAME_REGEX = "^[a-zA-Z_]\\w*$"
    /** The regular expression matcher for [.VALID_NAME_REGEX].  */
    private val VALID_NAME_PATTERN = Pattern.compile(VALID_NAME_REGEX)

    /**
     * Validate a potential column name for use in a Schema. Valid names are given by [.VALID_NAME_REGEX].

     * @param name the candidate column name.
     * *
     * @return the supplied name, if it is valid.
     * *
     * @throws IllegalArgumentException if the name does not match the regex [.VALID_NAME_REGEX].
     */
    private fun checkName(name: String): String {
      Preconditions.checkArgument(
          VALID_NAME_PATTERN.matcher(name).matches(),
          "supplied column name %s does not match the valid name regex %s",
          name,
          VALID_NAME_REGEX)
      return name
    }

  }

//  /**
//   * Return true if the two schema are "compatible": they have the same size and column types; column names are ignored.
//   */
//  fun compatible(a2: Attributes): Boolean {
//    return attributes.size == a2.attributes.size &&
//        attributes.zip(a2.attributes).all { it.first.encoder == it.second.encoder }
//  }


//  @Transient
  val nameToIndex: Map<Name, Int> by lazy {
    ImmutableMap.copyOf(attributes.mapIndexed { i, attribute -> attribute.name to i }.toMap())
  }

  /**
   * Find the index of the column with a given name.
   * @throws NoSuchElementException if no column with a matching name is found.
   */
  fun columnNameToIndex(name: String): Int =
      nameToIndex[name] ?: throw NoSuchElementException("No column named $name found; names are ${nameToIndex.keys}")

  /**
   * Return a subset of the current schema.
   * @param idxs indices to be selected.
   */
  fun getSubAttribtues(idxs: IntArray): RelationSchema =
      RelationSchema(attributes.slice(idxs.asIterable()))

  override fun toString(): String{
    return "RelationSchema(attributes=$attributes)"
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as RelationSchema

    if (attributes != other.attributes) return false

    return true
  }

  override fun hashCode(): Int{
    return attributes.hashCode()
  }


}

open class Schema protected constructor(
    val keyAttributes: ImmutableList<Attribute<*>>,
    val valAttribtues: ImmutableList<Attribute<*>>
) : RelationSchema(ImmutableList.builder<Attribute<*>>().addAll(keyAttributes).addAll(valAttribtues).build()) {

  constructor(kas: Collection<Attribute<*>>, vas: Collection<Attribute<*>>)
  : this(ImmutableList.copyOf(kas), ImmutableList.copyOf(vas))

}

open class AccessPath(
    /** distributed access path */
    val dap: ImmutableList<Attribute<*>>,
    /** local access path */
    val lap: ImmutableList<Attribute<*>>,
    /**
     * column access path
     * A list of the attribute groups. Each group is potentially stored in a different file.
     * The ordering of attributes within groups is lexicographic.
     */
    val cap: ImmutableList<ColumnFamily>
) : Schema(
    ImmutableList.builder<Attribute<*>>()
        .addAll(dap)
        .addAll(lap)
        .build(),
    ImmutableList.builder<Attribute<*>>()
        .addAll(cap.flatMap { it.attributes })
        .build()
) {
  init {
    Preconditions.checkArgument(cap.sumBy { it.attributes.count() } == valAttribtues.size,
        "one of the attributes was mentioned twice in two separate column families", cap)
  }
}


interface Tuple {

}

