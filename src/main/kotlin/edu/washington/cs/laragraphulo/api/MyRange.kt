package edu.washington.cs.laragraphulo.api

import com.google.common.collect.BoundType.*
import com.google.common.base.Preconditions.checkNotNull
import com.google.common.collect.*
import org.apache.accumulo.core.data.Key
import java.io.Serializable
import java.util.SortedSet

/**
 * See [com.google.common.collect.Range].
 * This class uses a [Comparator] rather than natural ordering.
 */
class MyRange<T> private constructor(
    val lower: T?,
    val lowerType: BoundType?,
    val upper: T?,
    val upperType: BoundType?
) : Serializable {
  companion object {
    fun <T> open(lower: T, upper: T) = MyRange(lower, OPEN, upper, OPEN)
    fun <T> closed(lower: T, upper: T) = MyRange(lower, CLOSED, upper, CLOSED)
    fun <T> openClosed(lower: T, upper: T) = MyRange(lower, OPEN, upper, CLOSED)
    fun <T> closedOpen(lower: T, upper: T) = MyRange(lower, CLOSED, upper, OPEN)
    fun <T> range(lower: T, lowerType: BoundType, upper: T, upperType: BoundType) = MyRange(lower, lowerType, upper, upperType)
    fun <T> lessThan(upper: T) = MyRange(null, null, upper, OPEN)
    fun <T> atMost(upper: T) = MyRange(null, null, upper, CLOSED)
    fun <T> upTo(upper: T, upperType: BoundType) = MyRange(null, null, upper, upperType)
    fun <T> greaterThan(lower: T) = MyRange(lower, OPEN, null, null)
    fun <T> atLeast(lower: T) = MyRange(lower, CLOSED, null, null)
    fun <T> downTo(lower: T, lowerType: BoundType) = MyRange(lower, lowerType, null, null)
    private val ALL: MyRange<Nothing> = MyRange(null,null,null,null)
    @Suppress("UNCHECKED_CAST")
    fun <T> all(): MyRange<T> = ALL as MyRange<T>
    fun <T> singleton(v: T) = closed(v,v)

    fun <T : Comparable<T>> fromGuava(r: com.google.common.collect.Range<T>) = fromGuava(r) {it}
    inline fun <I : Comparable<I>, R>
        fromGuava(r: com.google.common.collect.Range<I>, f: (I) -> R): MyRange<R> = when {
      !r.hasLowerBound() && !r.hasUpperBound() -> all()
      !r.hasLowerBound() -> when (r.upperBoundType()!!) {
        BoundType.CLOSED -> atMost(f(r.upperEndpoint()))
        BoundType.OPEN -> lessThan(f(r.upperEndpoint()))
      }
      !r.hasUpperBound() -> when (r.lowerBoundType()!!) {
        BoundType.CLOSED -> atLeast(f(r.lowerEndpoint()))
        BoundType.OPEN -> greaterThan(f(r.lowerEndpoint()))
      }
      else -> when (r.lowerBoundType()!!) {
        BoundType.CLOSED -> when (r.upperBoundType()!!) {
          BoundType.CLOSED -> closed(f(r.lowerEndpoint()), f(r.upperEndpoint()))
          BoundType.OPEN -> closedOpen(f(r.lowerEndpoint()), f(r.upperEndpoint()))
        }
        BoundType.OPEN -> when (r.upperBoundType()!!) {
          BoundType.CLOSED -> openClosed(f(r.lowerEndpoint()), f(r.upperEndpoint()))
          BoundType.OPEN -> open(f(r.lowerEndpoint()), f(r.upperEndpoint()))
        }
      }
    }
    fun <T : Comparable<T>> toGuava(r: MyRange<T>) = toGuava(r) {it}
    inline fun <I, R : Comparable<R>>
        toGuava(r: MyRange<I>, f: (I) -> R): com.google.common.collect.Range<R> = when {
          !r.hasLowerBound() && !r.hasUpperBound() -> com.google.common.collect.Range.all<R>()
          !r.hasLowerBound() -> when (r.upperBoundType()) {
            BoundType.CLOSED -> com.google.common.collect.Range.atMost(f(r.upperEndpoint()))
            BoundType.OPEN -> com.google.common.collect.Range.lessThan(f(r.upperEndpoint()))
          }
          !r.hasUpperBound() -> when (r.lowerBoundType()) {
            BoundType.CLOSED -> com.google.common.collect.Range.atLeast(f(r.lowerEndpoint()))
            BoundType.OPEN -> com.google.common.collect.Range.greaterThan(f(r.lowerEndpoint()))
          }
          else -> when (r.lowerBoundType()) {
            BoundType.CLOSED -> when (r.upperBoundType()) {
              BoundType.CLOSED -> com.google.common.collect.Range.closed(f(r.lowerEndpoint()), f(r.upperEndpoint()))
              BoundType.OPEN -> com.google.common.collect.Range.closedOpen(f(r.lowerEndpoint()), f(r.upperEndpoint()))
            }
            BoundType.OPEN -> when (r.upperBoundType()) {
              BoundType.CLOSED -> com.google.common.collect.Range.openClosed(f(r.lowerEndpoint()), f(r.upperEndpoint()))
              BoundType.OPEN -> com.google.common.collect.Range.open(f(r.lowerEndpoint()), f(r.upperEndpoint()))
            }
          }
        }

    fun fromAccumulo(r: org.apache.accumulo.core.data.Range): MyRange<Key> {
      val startKey: Key? = r.startKey
      val endKey: Key? = r.endKey
      val si = r.isStartKeyInclusive
      val ei = r.isEndKeyInclusive
      return when {
        startKey == null && endKey == null -> all<Key>()
        startKey == null && ei -> atMost(endKey!!)
        startKey == null -> lessThan(endKey!!)
        endKey == null && si -> atLeast(startKey)
        endKey == null -> greaterThan(startKey)
        si && ei -> closed(startKey,endKey)
        si -> closedOpen(startKey,endKey)
        ei -> openClosed(startKey,endKey)
      // possibly weird case if startKey == endKey; empty range; test this case
        else -> open(startKey,endKey)
      }
    }
    private fun BoundType.toBoolean() = when (this) {
      BoundType.CLOSED -> true
      BoundType.OPEN -> false
    }
    fun toAccumulo(r: MyRange<Key>): org.apache.accumulo.core.data.Range {
      return when {
        !r.hasLowerBound() && !r.hasUpperBound() -> org.apache.accumulo.core.data.Range()
        !r.hasLowerBound() -> org.apache.accumulo.core.data.Range(null, false, r.upperEndpoint(), r.upperBoundType().toBoolean())
        !r.hasUpperBound() -> org.apache.accumulo.core.data.Range(r.lowerEndpoint(), r.lowerBoundType().toBoolean(), null, false)
        else -> org.apache.accumulo.core.data.Range(r.lowerEndpoint(), r.lowerBoundType().toBoolean(), r.upperEndpoint(), r.upperBoundType().toBoolean())
      }
    }

    fun <C> encloseAll(comp0: Comparator<C>, values: Iterable<C>): MyRange<C> {
      val valueIterator = values.iterator()
      var min = checkNotNull(valueIterator.next())
      var max = min
      val comp = Ordering.from(comp0)
      while (valueIterator.hasNext()) {
        val value = checkNotNull(valueIterator.next())
        min = comp.min(min, value)
        max = comp.max(max, value)
      }
      return closed(min, max)
    }

  }

  init {
    require(lower == null && lowerType == null || lower != null && lowerType != null) {"lower and lowerType should match in nullness"}
    require(upper == null && upperType == null || upper != null && upperType != null) {"upper and upperType should match in nullness"}
  }

  fun hasLowerBound() = lower != null
  fun lowerEndpoint(): T { check(lower != null) {"unbounded below: $this"}; return lower!! }
  fun lowerBoundType(): BoundType { check(lower != null) {"unbounded below: $this"}; return lowerType!! }
  fun hasUpperBound() = upper != null
  fun upperEndpoint(): T { check(upper != null) {"unbounded above: $this"}; return upper!! }
  fun upperBoundType(): BoundType { check(upper != null) {"unbounded above: $this"}; return upperType!! }
  fun isEmpty(comp: Comparator<T>): Boolean = lower == null || upper == null || comp.compare(lower, upper) != 0 || lowerType == CLOSED || upperType == CLOSED
  fun <R> transform(f: (T) -> R): MyRange<R> = MyRange(if (lower == null) null else f(lower), lowerType,
      if (upper == null) null else f(upper), upperType)

  fun contains(comp: Comparator<T>, value: T): Boolean {
    // let this throw CCE if there is some trickery going on
    return (lower == null || comp.compare(lower, value).let { it < 0 || lowerType == CLOSED && it == 0 }) &&
        (upper == null || comp.compare(value, upper).let { it < 0 || upperType == CLOSED && it == 0 })
  }

  fun containsAll(comp: Comparator<T>, values: Iterable<T>): Boolean {
    if (Iterables.isEmpty(values)) return true

    // this optimizes testing equality of two range-backed sets
    if (values is SortedSet<T> && values.comparator() == comp) {
      return contains(comp, values.first()) && contains(comp, values.last())
    }

    return values.any { contains(comp, it) }
  }

  fun encloses(comp: Comparator<T>, other: MyRange<T>): Boolean {
    if (other.lower == null && lower != null) return false
    if (other.upper == null && upper != null) return false
    return (lower == null || comp.compare(lower, other.lower).let { it < 0 || it == 0 && (lowerType != OPEN || other.lowerType != CLOSED) }) &&
        (upper == null || comp.compare(other.upper, upper).let { it < 0 || it == 0 && (upperType != OPEN || other.upperType != CLOSED) })
  }

//  /** Is the  */
//  fun isConnected(other: MyRange<T>): Boolean {
//    if (lower == null && other. || upper == null || other.lower = null || other.upper == null)
//    return lowerBound.compareTo(other.upperBound) <= 0 && other.lowerBound.compareTo(upperBound) <= 0
//  }
  fun span(comp: Comparator<T>, r2: MyRange<T>): MyRange<T> {
    val minLower = when {
      lower == null -> this
      r2.lower == null -> r2
      else -> comp.compare(lower, r2.lower).let { if (it < 0 || lowerType == CLOSED && it == 0) this else r2 }
    }
    val maxUpper = when {
      upper == null -> this
      r2.upper == null -> r2
      else -> comp.compare(r2.upper, upper).let { if (it < 0 || upperType == CLOSED && it == 0) this else r2 }
    }
    return when {
      minLower === maxUpper -> minLower
      else -> MyRange(minLower.lower, minLower.lowerType, maxUpper.upper, maxUpper.upperType)
    }
  }
  fun intersection(comp: Comparator<T>, r2: MyRange<T>): MyRange<T>? {
    val maxLower = when {
      lower == null -> r2
      r2.lower == null -> this
      else -> comp.compare(lower, r2.lower).let { if (it < 0 || lowerType == CLOSED && it == 0) r2 else this }
    }
    val minUpper = when {
      upper == null -> r2
      r2.upper == null -> this
      else -> comp.compare(r2.upper, upper).let { if (it < 0 || upperType == CLOSED && it == 0) r2 else this }
    }
    // check well-defined via isConnected()
    // maxLower should not be greater than minUpper
    val valid = maxLower.lower == null || minUpper.upper == null || comp.compare(maxLower.lower, minUpper.upper).let { it < 0 || maxLower.lowerType == CLOSED && it == 0 }
    // this could be the empty range
    return when {
      !valid -> null
      maxLower === minUpper -> maxLower
      else -> MyRange(maxLower.lower, maxLower.lowerType, minUpper.upper, minUpper.upperType)
    }
  }

  internal fun readResolve(): Any {
    if (this == ALL) {
      return all<Comparable<*>>()
    } else {
      return this
    }
  }

  override fun toString(): String {
    return if (lower == null) "(-∞" else {
      (if (lowerType!! == OPEN) "(" else "[") + lower.toString()
    } + ".." + if (upper == null) "∞)" else {
      upper.toString() + (if (upperType!! == OPEN) ")" else "]")
    }
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as MyRange<*>

    if (lower != other.lower) return false
    if (lowerType != other.lowerType) return false
    if (upper != other.upper) return false
    if (upperType != other.upperType) return false

    return true
  }

  override fun hashCode(): Int {
    var result = lower?.hashCode() ?: 0
    result = 31 * result + (lowerType?.hashCode() ?: 0)
    result = 31 * result + (upper?.hashCode() ?: 0)
    result = 31 * result + (upperType?.hashCode() ?: 0)
    return result
  }

  /** Advance an iterator until it peeks an element that occurs within this range's lower bound.
   * Not guaranteed to remain within this range's upper bound. */
  private fun advanceToRange(comp: Comparator<T>, iterator: PeekingIterator<T>) {
    if (lower == null) return
    while (iterator.hasNext() && comp.compare(iterator.peek(), lower).let { it < 0 || lowerType == CLOSED && it == 0 })
      iterator.next()
  }

  /** Restrict an iterator to only emit entries within the range. **Assumes the iterator is sorted.** */
  fun restrict(comp: Comparator<T>, iter: PeekingIterator<T>): PeekingIterator<T> = RangeRestrictedIterator(comp, iter)

  // idea: if iter is another RangeRestrictedIterator, then intersect the ranges (assuming comp compatible)
  private inner class RangeRestrictedIterator(val comp: Comparator<T>, val iter: PeekingIterator<T>) : PeekingIterator<T>  {
    init {
      advanceToRange(comp, iter)
    }
    var inRange: Boolean = checkInRange()
    fun checkInRange(): Boolean = iter.hasNext() && (upper == null || comp.compare(iter.peek(), upper).let { it < 0 || upperType == CLOSED && it == 0 })

    override fun hasNext(): Boolean = inRange
    override fun remove() = throw UnsupportedOperationException("remove not supported")
    override fun next(): T {
      if (!inRange) throw NoSuchElementException()
      val n = iter.next()
      inRange = checkInRange()
      return n
    }
    override fun peek(): T {
      if (!inRange) throw NoSuchElementException()
      return iter.peek()
    }
  }

}

///**
// * Implementation detail for the internal structure of [Range] instances. Represents
// * a unique way of "cutting" a "number line" (actually of instances of type `C`, not
// * necessarily "numbers") into two sections; this can be done below a certain value, above
// * a certain value, below all values or above all values. With this object defined in this
// * way, an interval can always be represented by a pair of `Cut` instances.
//
// * @author Kevin Bourrillion
// */
//internal abstract class Cut<C>(val endpoint: C?) : Serializable {
//  internal abstract fun isLessThan(comp: Comparator<C>, value: C): Boolean
//  internal abstract fun typeAsLowerBound(): BoundType
//  internal abstract fun typeAsUpperBound(): BoundType
//  internal abstract fun describeAsLowerBound(sb: StringBuilder)
//  internal abstract fun describeAsUpperBound(sb: StringBuilder)
//
//  // note: overriden by {BELOW,ABOVE}_ALL
//  fun compareTo(comp: Comparator<C>, that: Cut<C>): Int {
//    if (that === belowAll<C>()) return 1
//    if (that === aboveAll<C>()) return -1
//    val result = comp.compare(endpoint, that.endpoint)
//    if (result != 0) return result
//    // same value. below comes before above
//    return Booleans.compare(this is AboveValue<*>, that is AboveValue<*>)
//  }
//
//  internal open fun endpoint(): C {
//    return endpoint!!
//  }
//
//  override // catching CCE
//  fun equals(obj: Any?): Boolean {
//    if (obj is Cut<*>) {
//      // It might not really be a Cut<C>, but we'll catch a CCE if it's not
//      val that = obj as Cut<*>
//      try {
//        val compareResult = endpoint == that.endpoint
//        return compareResult == 0
//      } catch (ignored: ClassCastException) {
//      }
//
//    }
//    return false
//  }
//
//  private class BelowAll private constructor() : Cut<*>(null) {
//
//    override fun endpoint(): Nothing {
//      throw IllegalStateException("range unbounded on this side")
//    }
//
//    override fun isLessThan(comp: Comparator<*>, value: Any?): Boolean {
//      return true
//    }
//
//    override fun typeAsLowerBound(): BoundType {
//      throw IllegalStateException()
//    }
//
//    override fun typeAsUpperBound(): BoundType {
//      throw AssertionError("this statement should be unreachable")
//    }
//
//    override fun describeAsLowerBound(sb: StringBuilder) {
//      sb.append("(-\u221e")
//    }
//
//    override fun describeAsUpperBound(sb: StringBuilder) {
//      throw AssertionError()
//    }
//
//    override fun compareTo(comp: Comparator<Any?>, o: Cut<Any?>): Int {
//      return if (o === this) 0 else -1
//    }
//
//    override fun toString(): String {
//      return "-\u221e"
//    }
//
//    private fun readResolve(): Any {
//      return INSTANCE
//    }
//
//    companion object {
//      internal val INSTANCE = BelowAll()
//
//      private val serialVersionUID: Long = 0
//    }
//  }
//
//  private class AboveAll private constructor() : Cut<*>(null) {
//
//    override fun endpoint(): Nothing {
//      throw IllegalStateException("range unbounded on this side")
//    }
//
//    override fun isLessThan(comp: Comparator<*>, value: Any?): Boolean {
//      return false
//    }
//
//    override fun typeAsLowerBound(): BoundType {
//      throw AssertionError("this statement should be unreachable")
//    }
//
//    override fun typeAsUpperBound(): BoundType {
//      throw IllegalStateException()
//    }
//
//    override fun describeAsLowerBound(sb: StringBuilder) {
//      throw AssertionError()
//    }
//
//    override fun describeAsUpperBound(sb: StringBuilder) {
//      sb.append("+\u221e)")
//    }
//
//    override fun compareTo(comp: Comparator<*>, o: Cut<*>): Int {
//      return if (o === this) 0 else 1
//    }
//
//    override fun toString(): String {
//      return "+\u221e"
//    }
//
//    private fun readResolve(): Any {
//      return INSTANCE
//    }
//
//    companion object {
//      internal val INSTANCE = AboveAll()
//
//      private val serialVersionUID: Long = 0
//    }
//  }
//
//  private class BelowValue<C> internal constructor(endpoint: C) : Cut<C>(checkNotNull(endpoint)) {
//
//    override fun isLessThan(comp: Comparator<C>, value: C): Boolean {
//      return comp.compare(endpoint, value) <= 0
//    }
//
//    override fun typeAsLowerBound(): BoundType {
//      return BoundType.CLOSED
//    }
//
//    override fun typeAsUpperBound(): BoundType {
//      return BoundType.OPEN
//    }
//
//    override fun describeAsLowerBound(sb: StringBuilder) {
//      sb.append('[').append(endpoint)
//    }
//
//    override fun describeAsUpperBound(sb: StringBuilder) {
//      sb.append(endpoint).append(')')
//    }
//
//    override fun hashCode(): Int {
//      return endpoint!!.hashCode()
//    }
//
//    override fun toString(): String {
//      return "\\" + endpoint + "/"
//    }
//
//    companion object {
//
//      private val serialVersionUID: Long = 0
//    }
//  }
//
//  private class AboveValue<C> internal constructor(endpoint: C) : Cut<C>(checkNotNull(endpoint)) {
//
//    override fun isLessThan(value: C): Boolean {
//      return Range.compareOrThrow(endpoint, value) < 0
//    }
//
//    override fun typeAsLowerBound(): BoundType {
//      return BoundType.OPEN
//    }
//
//    override fun typeAsUpperBound(): BoundType {
//      return BoundType.CLOSED
//    }
//
//    override fun withLowerBoundType(boundType: BoundType, domain: DiscreteDomain<C>): Cut<C> {
//      when (boundType) {
//        BoundType.OPEN -> return this
//        BoundType.CLOSED -> {
//          val next = domain.next(endpoint)
//          return if (next == null) Cut.belowAll<C>() else belowValue(next)
//        }
//        else -> throw AssertionError()
//      }
//    }
//
//    override fun withUpperBoundType(boundType: BoundType, domain: DiscreteDomain<C>): Cut<C> {
//      when (boundType) {
//        BoundType.OPEN -> {
//          val next = domain.next(endpoint)
//          return if (next == null) Cut.aboveAll<C>() else belowValue(next)
//        }
//        BoundType.CLOSED -> return this
//        else -> throw AssertionError()
//      }
//    }
//
//    override fun describeAsLowerBound(sb: StringBuilder) {
//      sb.append('(').append(endpoint)
//    }
//
//    override fun describeAsUpperBound(sb: StringBuilder) {
//      sb.append(endpoint).append(']')
//    }
//
//    override fun hashCode(): Int {
//      return endpoint!!.hashCode().inv()
//    }
//
//    override fun toString(): String {
//      return "/" + endpoint + "\\"
//    }
//
//    companion object {
//
//      private val serialVersionUID: Long = 0
//    }
//  }
//
//  companion object {
//
//    /*
//   * The implementation neither produces nor consumes any non-null instance of type C, so
//   * casting the type parameter is safe.
//   */
//    fun <C> belowAll(): Cut<C> {
//      return BelowAll.INSTANCE as Cut<C>
//    }
//
//    private const val serialVersionUID: Long = 0
//
//    /*
//   * The implementation neither produces nor consumes any non-null instance of
//   * type C, so casting the type parameter is safe.
//   */
//    fun <C> aboveAll(): Cut<C> {
//      return AboveAll.INSTANCE as Cut<C>
//    }
//
//    fun <C> belowValue(endpoint: C): Cut<C> {
//      return BelowValue(endpoint)
//    }
//
//    fun <C> aboveValue(endpoint: C): Cut<C> {
//      return AboveValue(endpoint)
//    }
//  }
//}




