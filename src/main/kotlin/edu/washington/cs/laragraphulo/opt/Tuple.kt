package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.*
import org.apache.accumulo.core.data.ArrayByteSequence
import java.util.*
import java.util.function.Function
import java.util.regex.Pattern
import kotlin.comparisons.nullsLast


// I am leaning toward storing attribute data separately (a list/array of names, a separate one for types, etc.)

sealed class RelationSchema(
    val attributes: ImmutableList<Name>
) {
  init {
    // check for duplicate names
    val names = attributes.toSet()
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

    fun build(attrs: Collection<Name>): RelationSchema = RelationSchemaImpl(attrs)
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
    ImmutableMap.copyOf(attributes.mapIndexed { i, attribute -> attribute to i }.toMap())
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



  private class RelationSchemaImpl(attrs: Collection<Name>)
  : RelationSchema(ImmutableList.copyOf(attrs))
}



sealed class Schema(
    val keyAttributes: ImmutableList<Name>,
    val valAttribtues: ImmutableList<Name>
) : RelationSchema(ImmutableList.builder<Name>().addAll(keyAttributes).addAll(valAttribtues).build()) {

  companion object {
    fun build( kas: Collection<Name>,
               vas: Collection<Name>): Schema = SchemaImpl(kas, vas)
  }

  private class SchemaImpl(
      kas: Collection<Name>,
      vas: Collection<Name>
  ) : Schema(ImmutableList.copyOf(kas), ImmutableList.copyOf(vas))

}



sealed class AccessPath(
    /** distributed access path */
    val dap: ImmutableList<Name>,
    /** local access path */
    val lap: ImmutableList<Name>,
    /**
     * column access path
     * A list of the attribute groups. Each group is potentially stored in a different file.
     * The ordering of attributes within groups is lexicographic.
     */
    val cap: ImmutableList<ColumnFamily>
) : Schema(
    ImmutableList.builder<Name>()
        .addAll(dap)
        .addAll(lap)
        .build(),
    ImmutableList.builder<Name>()
        .addAll(cap.flatMap { it.attributes })
        .build()
) {
  init {
    Preconditions.checkArgument(cap.sumBy { it.attributes.count() } == valAttribtues.size,
        "one of the attributes was mentioned twice in two separate column families", cap)
  }

  companion object {
    fun build( dap: Collection<Name>,
               lap: Collection<Name>,
               cap: Collection<ColumnFamily>): AccessPath = AccessPathImpl(dap,lap,cap)
  }

  private class AccessPathImpl(
      dap: Collection<Name>,
      lap: Collection<Name>,
      cap: Collection<ColumnFamily>
  ) : AccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap), ImmutableList.copyOf(cap))
}




// need to subclass AccessPath because this tells us how to interpret each part of the Key/Value
sealed class BagAccessPath(
    /** distributed access path */
    dap: ImmutableList<Name>,
    /** local access path */
    lap: ImmutableList<Name>,
    /**
     * column access path
     * A list of the attribute groups. Each group is potentially stored in a different file.
     * The ordering of attributes within groups is lexicographic.
     */
    cap: ImmutableList<ColumnFamily>,
    /** An int such that all [keyAttributes] whose index is less than sortedUpto are sorted.
     * 0 means nothing is sorted. Valid up to and including [dap].size+[lap].size. */
    val sortedUpto: Int,
    /** Whether there are multiple tuples with the same key attribute values. */
    val duplicates: Boolean
) : AccessPath(dap, lap, cap) {
  init {
    Preconditions.checkPositionIndex(sortedUpto, dap.size+lap.size, "sortedUpto is an int such that all keyAttributes $keyAttributes " +
        "whose index is less than sortedUpto are sorted. 0 means nothing is sorted. Valid up to and including ${dap.size+lap.size}. Given: $sortedUpto")
  }

  companion object {
    fun build( dap: Collection<Name>,
               lap: Collection<Name>,
               cap: Collection<ColumnFamily>,
               sortedUpto: Int,
               duplicates: Boolean): BagAccessPath = BagAccessPathImpl(dap,lap,cap,sortedUpto,duplicates)
  }

  private class BagAccessPathImpl(
      dap: Collection<Name>,
      lap: Collection<Name>,
      cap: Collection<ColumnFamily>,
      sortedUpto: Int,
      duplicates: Boolean
  ) : BagAccessPath(ImmutableList.copyOf(dap), ImmutableList.copyOf(lap), ImmutableList.copyOf(cap), sortedUpto, duplicates)
}









interface RelaxedTuple {
  operator fun get(name: Name): Any?
  operator fun get(idx: Int): Any?
  operator fun set(name: Name, v: Any?)
  operator fun set(idx: Int, v: Any?)
}

interface Tuple : RelaxedTuple {
  override operator fun get(name: Name): ArrayByteSequence
  override operator fun get(idx: Int): ArrayByteSequence
  @Deprecated("Use the ArrayByteSequence version", ReplaceWith("if (v is ArrayByteSequence) set(name, v) else throw IllegalArgumentException(\"\$v is not a ArrayByteSequence\")", "org.apache.accumulo.core.data.ArrayByteSequence"), DeprecationLevel.ERROR)
  override fun set(name: String, v: Any?) {
    if (v is ArrayByteSequence) set(name, v)
    else throw IllegalArgumentException("$v is not a ArrayByteSequence")
  }
  @Deprecated("Use the ArrayByteSequence version", ReplaceWith("if (v is ArrayByteSequence) set(idx, v) else throw IllegalArgumentException(\"\$v is not a ArrayByteSequence\")", "org.apache.accumulo.core.data.ArrayByteSequence"), DeprecationLevel.ERROR)
  override fun set(idx: Int, v: Any?) {
    if (v is ArrayByteSequence) set(idx, v)
    else throw IllegalArgumentException("$v is not a ArrayByteSequence")
  }
  operator fun set(name: Name, v: ArrayByteSequence)
  operator fun set(idx: Int, v: ArrayByteSequence)
}


class MutableByteTuple(
    val ap: AccessPath,
    /** The order of the buffers must match the order of the attributes in [ap] */
    private val buffers: MutableList<ArrayByteSequence>
): Tuple {
  init {
    // there is a ArrayByteSequence for every attribute
    Preconditions.checkArgument(buffers.size == ap.attributes.size,
        "expected %s data buffers but was given %s", ap.attributes.size, buffers.size)
  }

  override operator fun get(idx: Int): ArrayByteSequence = buffers[idx]
  override operator fun get(name: String): ArrayByteSequence = get(ap.columnNameToIndex(name))
  override operator fun set(idx: Int, v: ArrayByteSequence) {
    buffers[idx] = v
  }
  override operator fun set(name: String, v: ArrayByteSequence) = set(ap.columnNameToIndex(name), v)

  // could define a constructor that takes a map of names to ArrayByteSequences
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
      val b1 = t1[it]
      val b2 = t2[it]
      val c = b1.compareTo(b2)
      //WritableComparator.compareBytes(b1.array(), b1.arrayOffset() + b1.position(), b1.remaining(),
        //  b2.array(), b2.arrayOffset() + b2.position(), b2.remaining())
      if (c != 0)
        return@compare c
    }
    return 0
  }
}

data class TupleComparatorByAttributes(val attrs: ImmutableList<Name>, val reverse: Boolean = false) : Comparator<Tuple> {

  constructor(attrs: Collection<Name>): this(ImmutableList.copyOf(attrs))

  override fun compare(t1: Tuple, t2: Tuple): Int {
    attrs.forEach {
      val b1 = t1[it]
      val b2 = t2[it]
      val c = b1.compareTo(b2)
//      WritableComparator.compareBytes(b1.array(), b1.arrayOffset() + b1.position(), b1.remaining(),
//          b2.array(), b2.arrayOffset() + b2.position(), b2.remaining())
      if (c != 0)
        return@compare if (reverse) -c else c
    }
    return 0
  }
}

data class TupleIteratorComparatorByAttributes(val attrs: ImmutableList<Name>, val reverse: Boolean = false) : Comparator<PeekingIterator<out Tuple>> {
  val tcomp = nullsLast(TupleComparatorByAttributes(attrs, reverse)) // nulls always last

  constructor(attrs: Collection<Name>): this(ImmutableList.copyOf(attrs))

  override fun compare(t1: PeekingIterator<out Tuple>, t2: PeekingIterator<out Tuple>): Int =
      tcomp.compare(t1.peek(), t2.peek())
}


/** Todo: revise this for the SKVI version of seek that takes a range, column families, inclusive */
interface SeekableIterator<T> : Iterator<T> {
  fun seek(seekKey: T)
}


// later this will need to be a full interface, so that subclasses can maintain state
typealias MultiplyOp = (Array<Tuple>) -> Iterator<Tuple>

interface Collider {
  fun schema(inputs: List<BagAccessPath>): BagAccessPath
  /** Do NOT modify the contents of [actives]. */
  fun collide(inputs: List<Iterator<Tuple>>, actives: BooleanArray): Iterator<Tuple>
}

/**
 * This function assumes that the [commonKeys] are at the prefix of ever input.
 * I assume no inputs are aliased, and the commonKeys do not contain duplicates.
 *
 * This iterator eagerly reads the input iterators in order to cache the first element.
 */
class Merger(
    inputs: List<Iterator<Tuple>>,
    commonKeys: List<Name>,
    val collider: Collider,
    emitNoMatches: Set<Int>
): Iterator<Tuple> {
  private val inputs: ImmutableList<PeekingIterator<Tuple>> = inputs.fold(ImmutableList.builder<PeekingIterator<Tuple>>()) { builder, input -> builder.add(Iterators.peekingIterator(input)) }.build()
  val commonNames: ImmutableList<String> = commonKeys.fold(ImmutableList.builder<Name>()) { builder, input -> builder.add(input) }.build()
  private val emitNoMatches = BooleanArray(inputs.size) //ImmutableSet.copyOf(emitNoMatches)

  val inputComparator = TupleIteratorComparatorByAttributes(commonNames)
  val inputIndexComparator: Comparator<Int> = java.util.Comparator.comparing(
      Function({ it:Int -> this.inputs[it] }), inputComparator)
  /** A priority queue of indexes, referencing [inputs] and [emitNoMatches] */
  val pq: PriorityQueue<Int> = PriorityQueue(inputs.size, inputIndexComparator)
  var topIter: Iterator<Tuple> = Iterators.emptyIterator()

  init {
    // check that emitNoMatches is a valid set - every index corresponds to an input
    emitNoMatches.forEach {
      Preconditions.checkElementIndex(it, inputs.size, "emitNoMatch index $it is out of range; provided ${inputs.size} inputs")
      this.emitNoMatches[it] = true
    }
    pq.addAll(inputs.indices)
    findTop()
  }

  private val _actives = BooleanArray(inputs.size)
  private var _collision  = false
  private var _allFinished = false
  /** Todo: test that these indexes are the *least*. If not, reverse the comparator.
   * Sets state variables [_actives] and [_collision] according to the active set of indices and whether they trigger a collision.
   * */
  private fun pollActives() {
//    _actives.fill(false)
    val top = pq.poll()
    // return all falses if no iterators have any more elements left
    if (!inputs[top].hasNext()) {
      _allFinished = true
      return
    }
    _actives[top] = true
    _collision = _collision || this.emitNoMatches[top]
    while (pq.isNotEmpty() && inputIndexComparator.compare(top, pq.peek()) == 0)
      _actives[pq.poll()] = true
    if (pq.isEmpty()) _collision = true
    // all active inputs have hasNext() == true
  }

  /**
   * Re-add the active indexes into the priority queue, after advancing them to at least the least element in the queue.
   */
  private fun advanceActivesIntoQueue() {
    // all active inputs have hasNext() == true
    val toAdvanceTo: PeekingIterator<Tuple>? = pq.poll()?.let { inputs[it] }
    for ((idx, active) in _actives.withIndex()) {
      if (!active) continue
      // todo: this can be optimized further to account for the emitNoMatch criteria
      if (toAdvanceTo == null)
        inputs[idx].next()
      else
        advanceTo(inputs[idx], toAdvanceTo)
      pq.add(idx)
      _actives[idx] = false
    }
    _collision = false
    // post-condition: actives is filled with false, no collision
  }

  /** Primitive version without seeking */
  private fun advanceTo(input: PeekingIterator<Tuple>, toAdvanceTo: PeekingIterator<Tuple>) {
    do {
      input.next()
    } while (input.hasNext() && inputComparator.compare(input, toAdvanceTo) < 0)
  }

  private fun findTop() {
    do {
      pollActives()
      while (!_allFinished && !_collision) {
        advanceActivesIntoQueue()
        pollActives()
      }
      if (_allFinished)
        return
      topIter = collider.collide(inputs, _actives)
    } while (!topIter.hasNext())
  }

  override fun hasNext(): Boolean = topIter.hasNext()

  override fun next(): Tuple {
    val next = topIter.next()
    if (!topIter.hasNext())
      findTop()
    return next
  }
}

fun commonKeyNames(schemas: List<Schema>): Set<Name> =
    schemas.map { it.keyAttributes.map { it }.toSet() }.reduce { s1, s2 -> s1.intersect(s2) }

fun ensureKeyNamesSortedAtPrefix(schemas: List<BagAccessPath>, names: Set<Name>): List<Name> {
  if (schemas.isEmpty()) return names.toList()
  val bag1 = schemas[0]
  val nl: List<Name> = bag1.keyAttributes.subList(0, names.size).map { it }
  Preconditions.checkArgument(nl.toSet() == names, "names %s must be in the prefix of each itherator, but the first iterator has a prefix of %s", names, nl)
  schemas.forEach {
    Preconditions.checkArgument(it.keyAttributes.subList(0, names.size).map { it } == nl, "all iterators must have the same prefix key attributes; expected %s but actual %s", nl, it.keyAttributes)
    Preconditions.checkArgument(it.sortedUpto >= names.size, "all iterators must be sorted at least up to the names %s; this one is sorted on the first %s elements of %s", names, it.sortedUpto, it.keyAttributes)
  }
  return nl
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
  val schemaNames: List<List<Triple<Int, Int, String>>> = schemas.mapIndexed { sidx, schema -> schema.keyAttributes.mapIndexed { idx, attr -> Triple(sidx, idx, attr) } }
  // todo: need helper method to make sure that attributes with the same name are compatible (same type, etc.)
  val commonNames: List<Triple<Int, Int, String>> = schemaNames.reduce { pa, pb -> pa.filter { ita -> pb.any { it.third == ita.third } } }
  val resultKeyNames: List<Triple<Int, Int, String>> = schemaNames.fold(commonNames) { acc, names -> acc + (names.filterNot { itn -> acc.any { it.third == itn.third } })}
  val resultKeyAttributes: List<Name> = resultKeyNames.map { schemas[it.first].keyAttributes[it.second] }
  val resultSchema: Schema = Schema.build(resultKeyAttributes, multiplyOpValSchema.attributes)

  // assert that the input Iterator<Tuple>s are sorted in the right way...

  val comp = Comparator<Tuple> { o1, o2 ->
    // tuples must match on all common key attributes
    commonNames.forEach {
      val b1 = o1[it.third]
      val b2 = o2[it.third]
      val c = b1.compareTo(b2)
//      WritableComparator.compareBytes(b1.array(), b1.arrayOffset() + b1.position(), b1.remaining(),
//          b2.array(), b2.arrayOffset() + b2.position(), b2.remaining())
      if (c != 0)
        return@Comparator c
    }
    0
  }

  // todo - replace this with code a la MergeJoin. This code just merges; it doesn't multiply
  return resultSchema to Iterators.mergeSorted(inputs.map { it.second }, comp)
}

// method to check that an iterator is sorted in the right way, on the fly
