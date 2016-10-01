package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.*
import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import java.io.Serializable
import java.util.*
import java.util.function.Function
import java.util.regex.Pattern
import kotlin.comparisons.compareBy



/** Mock version that does a fixed thing. */
class ApplyIterator(
    val parent: TupleIterator
) : TupleIterator {
  var topTuple: Tuple? = null

  companion object {
    val RESULT = ArrayByteSequence("result".toByteArray())
    val SRC = ArrayByteSequence("src".toByteArray())
    val DST = ArrayByteSequence("dst".toByteArray())
    val UNDER = '_'.toByte()
  }

  override fun seek(sk: TupleSeekKey) {
    parent.seek(sk)
  }

  private fun prepTop() {
    if (topTuple != null && parent.hasNext()) {
      val t = parent.peek()
      // src_dst
      val src = t.vals[SRC]!!.first().value
      val dst = t.vals[DST]!!.first().value
      val result = ByteArray(src.length()+dst.length()+1)
      System.arraycopy(src.backingArray, src.offset(), result, 0, src.length())
      result[src.length()] = UNDER
      System.arraycopy(dst.backingArray, dst.offset(), result, src.length()+1, dst.length())
      topTuple = TupleImpl(t.keys, t.family,
          ImmutableListMultimap.of(RESULT, FullValue(ArrayByteSequence(result), EMPTY, Long.MAX_VALUE)))
    }
  }

  override fun peek(): Tuple {
    prepTop()
    return topTuple!!
  }

  override fun next(): Tuple {
    prepTop()
    val t = topTuple!!
    parent.next()
    topTuple = null
    return t
  }

  override fun hasNext(): Boolean {
    return parent.hasNext()
  }

  override fun serializeState(): ByteArray {
    throw UnsupportedOperationException("not implemented")
  }

  override fun deepCopy(env: IteratorEnvironment): ApplyIterator {
    return ApplyIterator(parent.deepCopy(env))
  }
}





// later this will need to be a full interface, so that subclasses can maintain state
typealias MultiplyOp = (Array<Tuple>) -> Iterator<Tuple>

interface Collider {
  fun schema(inputs: List<SortedSchema>): SortedSchema
  /** Do NOT modify the contents of [actives]. */
  fun collide(inputs: List<PeekingIterator<Tuple>>, actives: BooleanArray): Iterator<Tuple>
}

/**
 * This function assumes that the [commonKeys] are at the prefix of ever input.
 * I assume no inputs are aliased, and the commonKeys do not contain duplicates.
 *
 * This iterator eagerly reads the input iterators in order to cache the first element.
 */
class Merger(
    inputs: List<Iterator<Tuple>>,
    prefixSize: Int,
    val collider: Collider,
    emitNoMatches: Set<Int> = setOf() // this could be a BooleanArray
): Iterator<Tuple> {
  private val inputs: ImmutableList<PeekingIterator<Tuple>> = inputs.fold(ImmutableList.builder<PeekingIterator<Tuple>>()) { builder, input -> builder.add(Iterators.peekingIterator(input)) }.build()
  private val emitNoMatches = BooleanArray(inputs.size) //ImmutableSet.copyOf(emitNoMatches)

  val inputComparator = TupleIteratorComparatorByPrefix(prefixSize)
  val inputIndexComparator: Comparator<Int> = java.util.Comparator.comparing(
      Function({ it:Int -> this.inputs[it] }), inputComparator)
  /** A priority queue of indexes, referencing [inputs] and [emitNoMatches] */
  val pq: PriorityQueue<Int> = PriorityQueue(inputs.size, inputIndexComparator)
  var topIter: Iterator<Tuple> = Collections.emptyIterator()

  private val _actives = BooleanArray(inputs.size)
  private var _collision  = false
  private var _allFinished = false

  init {
    // check that emitNoMatches is a valid set - every index corresponds to an input
    emitNoMatches.forEach {
      Preconditions.checkElementIndex(it, inputs.size, "emitNoMatch index $it is out of range; provided ${inputs.size} inputs")
      this.emitNoMatches[it] = true
    }
    pq.addAll(inputs.indices)
    findTopIter()
  }

  /** Todo: test that these indexes are the *least*. If not, reverse the comparator.
   * Sets state variables [_actives] and [_collision] according to the active set of indices and whether they trigger a collision.
   * */
  private fun pollActives() {
//    _actives.fill(false)
    val top = pq.poll()!!
    // return all false if no iterators have any more elements left
    if (!inputs[top].hasNext()) {
      _allFinished = true
      return
    }
    _actives[top] = true
    _collision = _collision || this.emitNoMatches[top] // collision if an active match is designated as emitNoMatch
    while (pq.isNotEmpty() && inputIndexComparator.compare(top, pq.peek()) == 0) {
      val p = pq.poll()
      _actives[p] = true
      _collision = _collision || this.emitNoMatches[p]
    }
    if (pq.isEmpty()) _collision = true // collision if all iterators are active
    // all active inputs have hasNext() == true
  }

  /**
   * Re-add the active indexes into the priority queue, after advancing them to at least the least element in the queue.
   */
  private fun advanceActives() {
    // all active inputs have hasNext() == true
    val toAdvanceTo: PeekingIterator<Tuple>? = pq.poll()?.let { inputs[it] }
    for ((idx, active) in _actives.withIndex()) {
      if (!active) continue
      // todo: this can be optimized further to account for the emitNoMatch criteria
      if (toAdvanceTo == null)
        inputs[idx].next()
      else
        advanceTo(inputs[idx], toAdvanceTo)
    }
  }

  private fun addActivesIntoQueue() {
    for ((idx, active) in _actives.withIndex()) {
      if (!active) continue
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

  private fun findTopIter() {
    if (inputs.isEmpty()) return
    do {
      pollActives()
      while (!_allFinished && !_collision) {
        advanceActives()
        addActivesIntoQueue()
        pollActives()
      }
      if (_allFinished)
        return
      topIter = collider.collide(inputs, _actives) // collider must advance the active inputs
      addActivesIntoQueue()
    } while (!topIter.hasNext())
  }

  override fun hasNext(): Boolean = topIter.hasNext()

  override fun next(): Tuple {
    val next = topIter.next()
    if (!topIter.hasNext())
      findTopIter()
    return next
  }
}

fun commonKeyNames(schemas: List<KeySchema>): Set<Name> =
    schemas.map { it.keyNames.toSet() }.reduce { s1, s2 -> s1.intersect(s2) }

fun ensureKeyNamesSortedAtPrefix(schemas: List<SortedSchema>, names: Set<Name>): List<Name> {
  if (schemas.isEmpty()) return names.toList()
  val bag1 = schemas[0]
  val nl: List<Name> = bag1.keyNames.subList(0, names.size)
  require(nl.toSet() == names) {"names $names must be in the prefix of each iterator, but the first iterator has a prefix of $nl"}
  schemas.forEach {
    require(it.keyNames.subList(0, names.size) == nl) {"all iterators must have the same prefix key attributes; expected $nl but actual ${it.keyNames}"}
    require(it.sortedUpto >= names.size) {"all iterators must be sorted at least up to the names $names; this one is sorted on the first ${it.sortedUpto} elements of ${it.keyNames}"}
  }
  return nl
}

fun ensureSamePrefix(schemas: List<KeySchema>, prefixSize: Int) {
  if (schemas.isEmpty()) return
  schemas.forEach { require(it.keyNames.size >= prefixSize) {"A schema has less than $prefixSize keys: ${it}"} }
  for (i in 0..prefixSize-1) {
    val s = schemas[0].keyNames[i]
    for (j in 1..schemas.size-1)
      require(s == schemas[j].keyNames[i]) {"The schemas differ in their first $prefixSize attributes: $schemas"}
  }
}

fun ensureSortedUpto(schemas: List<SortedSchema>, prefixSize: Int) {
  schemas.forEach { require(it.sortedUpto >= prefixSize) }
}

/**
 * @return List of tuples from [iter] up until iter is exhausted or the [rowComparator] indicates that [iter] as a different tuple
 */
fun readRow(
    /** See [TupleComparatorByKeyPrefix] */
    rowComparator: Comparator<Tuple>,
    iter: PeekingIterator<Tuple>
): List<Tuple> {
  check(iter.hasNext()) {"$iter should hasNext()"}
  val first = iter.peek()
  val list = LinkedList<Tuple>()
  do {
    list.add(iter.next())
  } while (iter.hasNext() && rowComparator.compare(first, iter.peek()) == 0)
  return list
}

class OneRowIterator<T>(val rowComparator: Comparator<T>,
                     private val iter: PeekingIterator<T>) : PeekingIterator<T> by iter {
  val firstTuple: T? = if (iter.hasNext()) iter.peek() else null

  override fun next(): T = if (hasNext()) iter.next() else throw NoSuchElementException("the iterator is past the original row $firstTuple")

  override fun hasNext(): Boolean = iter.hasNext() && rowComparator.compare(firstTuple, iter.peek()) == 0

  override fun peek(): T = if (hasNext()) iter.peek() else throw NoSuchElementException("the iterator is past the original row $firstTuple")
}





















































///*
//  0. check that common keys are in the front of every iterator *in the same order*
//  1. align tuples on common key (the Aligner)
//  2. Collider: with the postcondition to advance all iterators past the common keys
//     Returns an Iterator<Tuple> - the tuples must conform to the colliderSchema -
//  2a. If holding all in memory, put all in memory and pass the maps to
// */
//
///**
// * @return resulting schema and the iterator stream
// */
//fun mergeJoin(
//    multiplyOp: MultiplyOp,
//    /** The Schema of value attributes as returned by the multiplyOp.
//     * The [multiplyOp] should not include key attributes. */
//    multiplyOpValSchema: ImmutableKeySchema,
//    inputs: List<Pair<Schema, Iterator<Tuple>>>
//): Pair<Schema, Iterator<Tuple>> {
//  // new keyNames = union of existing keyNames
//  // equi-join on matching key attributes
//  val schemas: List<Schema> = inputs.map { it.first }
//  val schemaNames: List<List<Triple<Int, Int, String>>> = schemas.mapIndexed { sidx, schema -> schema.keyNames.mapIndexed { idx, attr -> Triple(sidx, idx, attr) } }
//  // todo: need helper method to make sure that attributes with the same name are compatible (same type, etc.)
//  val commonNames: List<Triple<Int, Int, String>> = schemaNames.reduce { pa, pb -> pa.filter { ita -> pb.any { it.third == ita.third } } }
//  val resultKeyNames: List<Triple<Int, Int, String>> = schemaNames.fold(commonNames) { acc, names -> acc + (names.filterNot { itn -> acc.any { it.third == itn.third } })}
//  val resultkeyNames: List<Name> = resultKeyNames.map { schemas[it.first].keyNames[it.second] }
//  val resultSchema: Schema = Schema.build(resultkeyNames, multiplyOpValSchema)
//
//  // assert that the input Iterator<Tuple>s are sorted in the right way...
//
//  val comp = Comparator<Tuple> { o1, o2 ->
//    // tuples must match on all common key attributes
//    commonNames.forEach {
//      val b1 = o1[it.second]
//      val b2 = o2[it.second]
//      val c = b1.compareTo(b2)
////      WritableComparator.compareBytes(b1.array(), b1.arrayOffset() + b1.position(), b1.remaining(),
////          b2.array(), b2.arrayOffset() + b2.position(), b2.remaining())
//      if (c != 0)
//        return@Comparator c
//    }
//    0
//  }
//
//  // todo - replace this with code a la MergeJoin. This code just merges; it doesn't multiply
//  return resultSchema to Iterators.mergeSorted(inputs.map { it.second }, comp)
//}
//
//// method to check that an iterator is sorted in the right way, on the fly


interface AccumuloLikeIterator<K,T> : PeekingIterator<T> {
  @Suppress("DeprecatedCallableAddReplaceWith")
  @Deprecated("unsupported", level = DeprecationLevel.ERROR)
  override fun remove() = throw UnsupportedOperationException("remove is not supported")

  fun seek(sk: K)
  fun deepCopy(env: IteratorEnvironment): AccumuloLikeIterator<K,T>
  /** @return null if it is not safe to serialize state */
  fun serializeState(): ByteArray?
  // an init() function?

  /** Return the next value. This is the same value that [peek] returns prior to calling next. */
  override fun next(): T

  /** Peek at the next value; do not advance the iteration */
  override fun peek(): T
}

interface KeyValueIterator : AccumuloLikeIterator<SeekKey,KeyValue> {
  override fun deepCopy(env: IteratorEnvironment): KeyValueIterator
}

interface TupleIterator : AccumuloLikeIterator<TupleSeekKey,Tuple> {
  override fun deepCopy(env: IteratorEnvironment): TupleIterator
}

data class SeekKey(
    val key: Key,
    val families: Collection<ArrayByteSequence>,
    val inclusive: Boolean
)

data class TupleSeekKey(
    val tupleKey: TupleKey,
    val families: Collection<ArrayByteSequence>,
    val inclusive: Boolean
)

abstract class AccumuloOp(args: List<Op<*>> = emptyList()) : Op<Tuple>(args), Serializable
{
  constructor(vararg args: Op<*>): this(args.asList())

  /**
   * @param parent The source iterator that one of the leaves will connect to.
   * @param options Execution-time environmental parameters, passed from client
   * @param env Execution-time Accumulo parameters
   */
  abstract fun construct(parent: AccumuloLikeIterator<*,*>, options: Map<String,String>, env: IteratorEnvironment): AccumuloLikeIterator<*,*>
}

