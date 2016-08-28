package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Iterators
import java.nio.ByteBuffer
import java.util.*
import java.util.regex.Pattern


// I am leaning toward storing attribute data separately (a list/array of names, a separate one for types, etc.)

sealed class RelationSchema(
    val attributes: ImmutableList<Attribute<*>>
) {
  init {
    // check for duplicate names
    val names = attributes.map { it.name }.toSet()
    Preconditions.checkArgument(names.size == attributes.size, "There is a duplicate attribute name: ", names)
    // check for invalid names
    names.forEach { checkName(it) }
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
  private val nameToIndex: Map<Name, Int> by lazy {
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
  open fun getSubAttribtues(idxs: IntArray): RelationSchema =
      RelationSchemaImpl(attributes.slice(idxs.asIterable()))


  operator fun contains(name: Name): Boolean = name in nameToIndex

  // consider overriding +, -


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



  class RelationSchemaImpl(attrs: Collection<Attribute<*>>)
  : RelationSchema(ImmutableList.copyOf(attrs))
}



sealed class Schema(
    val keyAttributes: ImmutableList<Attribute<*>>,
    val valAttribtues: ImmutableList<Attribute<*>>
) : RelationSchema(ImmutableList.builder<Attribute<*>>().addAll(keyAttributes).addAll(valAttribtues).build()) {


  class SchemaImpl(
      kas: Collection<Attribute<*>>,
      vas: Collection<Attribute<*>>
  ) : Schema(ImmutableList.copyOf(kas), ImmutableList.copyOf(vas))

}



sealed class AccessPath(
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

  class AccessPathImpl(
      dap: Collection<Attribute<*>>,
      lap: Collection<Attribute<*>>,
      cap: Collection<ColumnFamily>
  ) : AccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap), ImmutableList.copyOf(cap))
}














interface Tuple {
  operator fun get(name: Name): ByteBuffer
  operator fun get(idx: Int): ByteBuffer
  operator fun set(name: Name, v: ByteBuffer)
  operator fun set(idx: Int, v: ByteBuffer)
}


class MutableByteTuple(
    val ap: AccessPath,
    /** The order of the buffers must match the order of the attributes in [ap] */
    private val buffers: MutableList<ByteBuffer>
): Tuple {
  init {
    // there is a ByteBuffer for every attribute
    Preconditions.checkArgument(buffers.size == ap.attributes.size,
        "expected %s data buffers but was given %s", ap.attributes.size, buffers.size)
  }

  override operator fun get(idx: Int): ByteBuffer = buffers[idx]
  override operator fun get(name: String): ByteBuffer = get(ap.columnNameToIndex(name))
  override operator fun set(idx: Int, v: ByteBuffer) {
    buffers[idx] = v
  }
  override operator fun set(name: String, v: ByteBuffer) = set(ap.columnNameToIndex(name), v)

  // could define a constructor that takes a map of names to ByteBuffers
  // use the AP to put the buffers in the right order
}


typealias ExtFun = (Tuple) -> Iterator<Tuple>

/**
 * Assumes that the iterator is in the order specified by a correct AccessPath
 */
@Suppress("UNCHECKED_CAST")
fun Iterator<Tuple>.ext(f: ExtFun): Iterator<Tuple> {
  return Iterators.concat(Iterators.transform(this, f as (Tuple?) -> Iterator<Tuple>))
}



typealias MultiplyOp = (Array<Tuple>) -> Iterator<Tuple>

/**
 * @return resulting schema and the iterator stream
 */
fun mergeJoin(
    multiplyOp: MultiplyOp,
    /** The Schema of value attributes as returned by the multiplyOp.
     * The [multiplyOp] should not include key attributes. */
    multiplyOpValSchema: RelationSchema,
    inputs: List<Pair<Schema, Iterator<Tuple>>>
): Pair<Schema, Iterator<Tuple>> {
  // new keyAttributes = union of existing keyAttributes
  // equi-join on matching key attributes
  val schemas: List<Schema> = inputs.map { it.first }
  val schemaNames: List<List<Triple<Int, Int, String>>> = schemas.mapIndexed { sidx, schema -> schema.keyAttributes.mapIndexed { idx, attr -> Triple(sidx, idx, attr.name) } }
  // todo: need helper method to make sure that attributes with the same name are compatible (same type, etc.)
  val commonNames: List<Triple<Int, Int, String>> = schemaNames.reduce { pa, pb -> pa.filter { ita -> pb.any { it.third == ita.third } } }
  val resultKeyNames: List<Triple<Int, Int, String>> = schemaNames.fold(commonNames) { acc, names -> acc + (names.filterNot { itn -> acc.any { it.third == itn.third } })}
  val resultKeyAttributes: List<Attribute<*>> = resultKeyNames.map { schemas[it.first].keyAttributes[it.second] }
  val resultSchema: Schema = Schema.SchemaImpl(resultKeyAttributes, multiplyOpValSchema.attributes)

  // assert that the input Iterator<Tuple>s are sorted in the right way...

}

