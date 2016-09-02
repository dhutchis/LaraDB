package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Iterators
import org.apache.hadoop.io.WritableComparator
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

    fun build(attrs: Collection<Attribute<*>>): RelationSchema = RelationSchemaImpl(attrs)
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



  private class RelationSchemaImpl(attrs: Collection<Attribute<*>>)
  : RelationSchema(ImmutableList.copyOf(attrs))
}



sealed class Schema(
    val keyAttributes: ImmutableList<Attribute<*>>,
    val valAttribtues: ImmutableList<Attribute<*>>
) : RelationSchema(ImmutableList.builder<Attribute<*>>().addAll(keyAttributes).addAll(valAttribtues).build()) {

  companion object {
    fun build( kas: Collection<Attribute<*>>,
               vas: Collection<Attribute<*>>): Schema = SchemaImpl(kas, vas)
  }

  private class SchemaImpl(
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

  companion object {
    fun build( dap: Collection<Attribute<*>>,
               lap: Collection<Attribute<*>>,
               cap: Collection<ColumnFamily>): AccessPath = AccessPathImpl(dap,lap,cap)
  }

  private class AccessPathImpl(
      dap: Collection<Attribute<*>>,
      lap: Collection<Attribute<*>>,
      cap: Collection<ColumnFamily>
  ) : AccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap), ImmutableList.copyOf(cap))
}




// need to subclass AccessPath because this tells us how to interpret each part of the Key/Value
sealed class AccumuloAccessPath(
    /** distributed access path */
    dap: ImmutableList<Attribute<*>>,
    /** local access path */
    lap: ImmutableList<Attribute<*>>,
    /**
     * column access path
     * A list of the attribute groups. Each group is potentially stored in a different file.
     * The ordering of attributes within groups is lexicographic.
     */
    cap: ImmutableList<ColumnFamily>,
    /** An int such that all [keyAttributes] whose index is less than sortedUpto are sorted.
     * 0 means nothing is sorted. Valid up to and including [dap].size+[lap].size. */
    val sortedUpto: Int,
    /** Placeholder for union */
    val sumRequired: Any? = null
) : AccessPath(dap, lap, cap) {
  init {
    Preconditions.checkElementIndex(sortedUpto, dap.size+lap.size+1, "sortedUpto is an int such that all keyAttributes $keyAttributes " +
        "whose index is less than sortedUpto are sorted. 0 means nothing is sorted. Valid up to and including ${dap.size+lap.size}. Given: $sortedUpto")
  }

  companion object {
    fun build( dap: Collection<Attribute<*>>,
               lap: Collection<Attribute<*>>,
               cap: Collection<ColumnFamily>,
               sortedUpto: Int,
               sumRequired: Any?): AccumuloAccessPath = AccumuloAccessPathImpl(dap,lap,cap,sortedUpto,sumRequired)
  }

  private class AccumuloAccessPathImpl(
      dap: Collection<Attribute<*>>,
      lap: Collection<Attribute<*>>,
      cap: Collection<ColumnFamily>,
      sortedUpto: Int,
      sumRequired: Any?
  ) : AccumuloAccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap), ImmutableList.copyOf(cap), sortedUpto, sumRequired)
}









interface RelaxedTuple {
  operator fun get(name: Name): Any?
  operator fun get(idx: Int): Any?
  operator fun set(name: Name, v: Any?)
  operator fun set(idx: Int, v: Any?)
}

interface Tuple : RelaxedTuple {
  override operator fun get(name: Name): ByteBuffer
  override operator fun get(idx: Int): ByteBuffer
  @Deprecated("Use the ByteBuffer version", ReplaceWith("if (v is ByteBuffer) set(name, v) else throw IllegalArgumentException(\"\$v is not a ByteBuffer\")", "java.nio.ByteBuffer"), DeprecationLevel.ERROR)
  override fun set(name: String, v: Any?) {
    if (v is ByteBuffer) set(name, v)
    else throw IllegalArgumentException("$v is not a ByteBuffer")
  }
  @Deprecated("Use the ByteBuffer version", ReplaceWith("if (v is ByteBuffer) set(idx, v) else throw IllegalArgumentException(\"\$v is not a ByteBuffer\")", "java.nio.ByteBuffer"), DeprecationLevel.ERROR)
  override fun set(idx: Int, v: Any?) {
    if (v is ByteBuffer) set(idx, v)
    else throw IllegalArgumentException("$v is not a ByteBuffer")
  }
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

data class OrderByKeyKeyComparator(val schema: Schema) : Comparator<Tuple> {
  override fun compare(t1: Tuple, t2: Tuple): Int {
    schema.keyAttributes.forEach {
      val b1 = t1[it.name]
      val b2 = t2[it.name]
      val c = WritableComparator.compareBytes(b1.array(), b1.arrayOffset() + b1.position(), b1.remaining(),
          b2.array(), b2.arrayOffset() + b2.position(), b2.remaining())
      if (c != 0)
        return@compare c
    }
    return 0
  }
}


// later this will need to be a full interface, so that subclasses can maintain state
typealias MultiplyOp = (Array<Tuple>) -> Iterator<Tuple>

interface Collider {
  // TODO
}

/*
  0. check that common keys are in the front of every iterator *in the same order*
  1. align tuples on common key (the Aligner)
  2. Collider: with the postcondition to advance all iterators past the common keys
     Returns an Iterator<Tuple> - the tuples must conform to the colliderSchema -
  2a. If holding all in memory, put all in memory and pass the maps to
 */

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
  val resultSchema: Schema = Schema.build(resultKeyAttributes, multiplyOpValSchema.attributes)

  // assert that the input Iterator<Tuple>s are sorted in the right way...

  val comp = Comparator<Tuple> { o1, o2 ->
    // tuples must match on all common key attributes
    commonNames.forEach {
      val b1 = o1[it.third]
      val b2 = o2[it.third]
      val c = WritableComparator.compareBytes(b1.array(), b1.arrayOffset() + b1.position(), b1.remaining(),
          b2.array(), b2.arrayOffset() + b2.position(), b2.remaining())
      if (c != 0)
        return@Comparator c
    }
    0
  }

  // todo - replace this with code a la MergeJoin. This code just merges; it doesn't multiply
  return resultSchema to Iterators.mergeSorted(inputs.map { it.second }, comp)
}

// method to check that an iterator is sorted in the right way, on the fly
