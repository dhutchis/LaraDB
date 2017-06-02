package edu.washington.cs.laragraphulo.api

import com.google.common.collect.BoundType
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.util.GraphuloUtil.UnimplementedIteratorEnvironment
import edu.washington.cs.laragraphulo.warn
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.slf4j.Logger
import java.io.Serializable
import java.util.*

/**
 *
 */
sealed class TupleOp(private vararg val args: TupleOp) : Serializable {
  init {
    require(args.all {it !== this}) {"A TupleOp cannot have itself as a parent: $this"}
  }
  abstract val resultSchema: Schema
  fun run(): TupleIterator = run(UnimplementedIteratorEnvironment)
  fun run(env: IteratorEnvironment): TupleIterator = run(mutableMapOf(), env)
  fun run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator =
    instMap[this]?.deepCopy(env) ?: _run(instMap, env).also { instMap += this to it }
  protected abstract fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator
  /** Create a copy of this op with new parent TupleOps. */
  protected abstract fun reconstruct(args: Array<TupleOp>): TupleOp
  /** All subclasses should override equals() and hashCode() */
  abstract override fun hashCode(): Int
  /** All subclasses should override equals() and hashCode() */
  abstract override fun equals(other: Any?): Boolean

  /** Passed to [transformFold] to its function. */
  data class TransformParams<T>(
      val fromChild: T,
      val op: TupleOp,
      /** Pass number. On 0, has not yet visited any parents.
       * On 1, has visited 1st parent. On 2, has visted 2nd parent.
       * Does not exceed [maxPosition]. */
      val curPosition: Int,
      /** Length [maxPosition]. Contents are valid up to position [curPosition]-1. */
      val retArray: Array<T>
  ) {
    /** Number of parents. */
    val maxPosition: Int = retArray.size

    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (other?.javaClass != javaClass) return false

      other as TransformParams<*>

      if (op != other.op) return false
      if (curPosition != other.curPosition) return false
      if (maxPosition != other.maxPosition) return false
      if (!Arrays.equals(retArray, other.retArray)) return false

      return true
    }
    override fun hashCode(): Int {
      var result = op.hashCode()
      result = 31 * result + curPosition
      result = 31 * result + maxPosition
      result = 31 * result + Arrays.hashCode(retArray)
      return result
    }
  }
  /** Used by [transformFold]'s function to indicate what to do next.
   * Either (1) Continue with the next parent, (2) Reset and replace the current parent with another one, or
   * (3) finish the current operation and return a result. */
  sealed class TransformResult<out T> {
    /** Continue with next parent. If finished all parents, Stop and return the default.
     * The data [toParent] is passed to the next parent in [TransformParams]. */
    data class Continue<out T>(
        val toParent: T
    ): TransformResult<T>()
    // this is disabled until the complexity is warranted by a real use case
//    /** Replace the current op with this new one, and continue recursing with its [nextParentIndex] parent.
//     * Discards [TransformParams.retArray]. */
//    data class Reset(
//        val replacement: TupleOp,
//        val nextParentIndex: Int = 0
//    ) : TransformResult<Nothing>()
    /** Stop immediately and return [ret]. */
    data class Stop<out T>(
        val replacement: TupleOp,
        val ret: T
    ) : TransformResult<T>()
  }

  companion object : Loggable {
    override val logger: Logger = logger<TupleOp>()
    /** Helper function for [transformFold]. */
    inline fun <reified T> defaultArray(default: T): (Int) -> Array<T> = {Array<T>(it) {default}}
  }

  inline fun <reified T> transformFold(
      default: T, noinline f: (TransformParams<T>) -> TransformResult<T>
  ) = transformFold(default, defaultArray(default), f)

  /** Transform this TupleOp stack. Performs a visiting traversal. Each TupleOp is visited
   * before its parents (pre-order), in between its parents (in-order), and after its parents (post-order).
   * The function [f] is called at each of these times,
   * with [TransformParams.curPosition] parents being visited before the call.
   * The [TransformParams.retArray] has valid elements from [TransformParams.curPosition] parents
   * (that is, 0..[TransformParams.curPosition]-1).
   * [TransformParams.maxPosition] is the number of parents (this is the size of [TransformParams.retArray]).
   * As soon as [f] returns a [TransformResult.Stop] or it [TransformResult.Continue]s after visiting its last parent,
   * then transformFold finishes this operator and returns a result [T] to its child. */
  fun <T> transformFold(
      fromChild: T,
//      default: T,
      /** A function to create an array of default values of a given size. `null` entries are a decent option.
       * Use [defaultArray] to help construct this. */
      defaultRetArray: (Int) -> Array<T>,
      f: (TransformParams<T>) -> TransformResult<T>): TransformResult.Stop<T> {
    var op = this
    var curPos = 0
    val maxPos = args.size
    val retArr = defaultRetArray(maxPos)
    w@ while (curPos <= maxPos) {
      val tp = TransformParams(fromChild, op, curPos, retArr)
      val result = f(tp) // don't use ret unless we are returning
      val toParent = when (result) {
        is TransformResult.Stop -> return result
//        is TransformResult.Reset -> {
//          op = result.replacement
//          curPos = result.nextParentIndex
//          maxPos = op.args.size
//          retArr = defaultRetArray(maxPos)
//          continue@w
//        }
        is TransformResult.Continue -> result.toParent
      }
      if (curPos == maxPos) { // Continue but finished all parents
        logger.warn("Continue but finished all parents")
        return TransformResult.Stop(op, fromChild)
      }

      // visit parent # curPos
      val (replacement,ret) = op.args[curPos].transformFold(toParent, defaultRetArray, f)
      retArr[curPos] = ret
      if (replacement !== op.args[curPos]) { // if parent changed, replace parent
        val args2 = Arrays.copyOf(op.args, op.args.size)
        args2[curPos] = replacement
        op = op.reconstruct(args2)
      }
      curPos++
    }
    throw AssertionError()
  }
  
  fun transform(f: (TupleOp) -> TupleOp): TupleOp {
    val newArgs = Arrays.copyOf(args, args.size)
    var diff = false
    for (i in newArgs.indices) {
      newArgs[i] = newArgs[i].transform(f)
      diff = diff || newArgs[i] !== args[i]
    }
    return f(if (diff) reconstruct(newArgs) else this)
  }
  
//  /** Visit each op and run a function on it without altering it */
//  fun visit(f: (TupleOp) -> Unit) = transformFold { f(it); it }
  /** Do a structural fold over this TupleOp stack. [combine] should be **commutative**. */
  inline fun <reified T> fold(init: T, crossinline f: (TupleOp, Array<T>) -> T): T {
    val f1: (TransformParams<T>) -> TransformResult<T> = { (fc, op, curPos, retArray) ->
      if (retArray.size == curPos) TransformResult.Stop(op, f(op, retArray))
      else TransformResult.Continue(fc)
    }
    return transformFold(init, f1).ret
  }
  // todo next - revise Lower.kt transformFold







  fun ext(extFun: ExtFun) = Ext(this, extFun)
  fun map(mapFun: MapFun) = Ext(this, mapFun)
  data class Ext(
      val parent: TupleOp,
      /** This can also be a [MapFun] */
      val extFun: ExtFun
  ): TupleOp(parent) {
    override val resultSchema = Schema(
        keys = parent.resultSchema.keys + extFun.extSchema.keys,
        vals = extFun.extSchema.vals
    )
    override fun reconstruct(args: Array<TupleOp>) = if (args[0] === parent) this else copy(parent = args[0])
    fun reconstruct(p: TupleOp) = if (p === parent) this else copy(parent = p)

    init {
      require(extFun.extSchema.keys.disjoint(parent.resultSchema.keys))
      {"ext generates keys that are already present in the parent. Ext: ${extFun.extSchema}. Parent schema: ${parent.resultSchema}."}
    }

    /*    companion object {
//      fun runExtFunctionOnDefaultValues(ps: Schema, f: ExtFun): List<ValAttribute<*>> {
//        val tuple = (ps.keys.map { it.name to it.type.examples.first() } +
//            ps.vals.map { it.name to it.default }).toMap()
//        val result = f.extFun(tuple)
//        if (result.isEmpty()) {
//          require()
//        }
//        f.extVals.map { va ->
//          require(va.name in result)
//        }
//      }
//    } */
    val parentKeyNames = parent.resultSchema.keys.map { it.name }

    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator =
        ExtIterator(parent.run(instMap, env), extFun, parentKeyNames)

    class ExtIterator(
        val iter: TupleIterator,
        val extFun: ExtFun,
        val parentKeyNames: List<Name>
    ) : TupleIterator {
      private var top: PeekingIterator<Tuple> by Staged { findTop() }

      fun findTop(): PeekingIterator<Tuple> {
        if (!iter.hasNext())
          return Collections.emptyIterator<Tuple>().peeking()
        var topIter: Iterator<Tuple>
        var topParent: Tuple
        do {
          topParent = iter.next()
          topIter = extFun.extFun(topParent).iterator()
        } while (iter.hasNext() && !topIter.hasNext())
        return PrependKeysIterator(parentKeyNames, topParent, topIter).peeking() // could reuse this object for performance
      }

      override fun hasNext(): Boolean = top.hasNext()
      override fun next(): Tuple {
        val r = top.next()
        if (!top.hasNext()) top = findTop()
        return r
      }
      override fun peek(): Tuple = top.peek()
      override fun seek(seek: TupleSeekKey) {
        // eliminate extra generated keys from seek keys
        val newSeek = if (extFun.extSchema.keys.isNotEmpty())
          seek.copy(range = seek.range.transform { it.filterKeys { it !in extFun.extSchema.keys.map(Attribute<*>::name) } })
        else seek
        iter.seek(newSeek)
        top = findTop()
      }
      override fun deepCopy(env: IteratorEnvironment): TupleIterator = ExtIterator(iter.deepCopy(env), extFun, parentKeyNames)
    }

    private class PrependKeysIterator(
        keysToPrepend: List<String>,
        parent: Tuple,
        val iter: Iterator<Tuple>
    ) : Iterator<Tuple> {
      val parentKeys = parent.filterKeys { it in keysToPrepend }
      override fun hasNext(): Boolean = iter.hasNext()
      override fun next(): Tuple {
        val n = iter.next().filterKeys { it !in parentKeys }
//        check(parentKeys.keys.all { it !in n }) {"the tuple resulting from this ext emitted a key that is present in the parent keys. Tuple: $n. ParentKeys: $parentKeys"}
        return parentKeys + n
      }
    }


  }

  data class Load(
      val table: Table,
      override val resultSchema: Schema
  ): TupleOp() {
    //    constructor(table: String, schema: Schema, iter: Iterator<NameTuple>): this(table, schema, Collections.emptyIterator())
    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment) = throw UnsupportedOperationException("Cannot run a Load() Op; need to provide a data source for this: $this")
    override fun reconstruct(args: Array<TupleOp>) = this
    override fun toString() = "Load(table='$table')"

  }

  data class Empty(
      override val resultSchema: Schema
  ) : TupleOp() {
    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment) = TupleIterator.EMPTY
    override fun reconstruct(args: Array<TupleOp>) = this
  }




  fun union(p2: TupleOp, plusFuns: Map<Name, PlusFun<*>>) = MergeUnion0(this, p2, plusFuns)
  fun agg(keysKept: Collection<Name>, plusFuns0: Map<Name, PlusFun<*>>) = MergeUnion0.MergeAgg(this, keysKept, plusFuns0)
  /**
   * Restricted to two parents. Future work could extend this to any number of parents.
   */
  // todo - rename to MergeUnion
  class MergeUnion0(
      val p1: TupleOp,
      val p2: TupleOp,
      plusFuns0: Map<Name, PlusFun<*>>
  ): TupleOp(p1, p2) {
    override val resultSchema = Schema(
        keys = intersectKeys(p1.resultSchema.keys,p2.resultSchema.keys),
        vals = unionValues(p1.resultSchema.vals,p2.resultSchema.vals)
    )
    override fun reconstruct(args: Array<TupleOp>): MergeUnion0 = if (args[0] === p1 && args[1] === p2) this else args[0].union(args[1], plusFuns)
    fun reconstruct(p1: TupleOp, p2: TupleOp): MergeUnion0 = if (p1 === this.p1 && p2 === this.p2) this else p1.union(p2, plusFuns)

    init {
      require(resultSchema.vals.map(ValAttribute<*>::name).containsAll(plusFuns0.keys)) {"plus functions provided for values that do not exist"}
      plusFuns0.forEach { name, pf ->
        val d = resultSchema.vals.find { it.name == name }!!.default
        pf.verifyIdentity()
        require(pf.identity == d) {"plus function for $name does not match identity of parent: $d"}
      }
    }

    val plusFuns: Map<Name, PlusFun<*>> = resultSchema.vals.map { va ->
      val pf = plusFuns0[va.name] ?: PlusFun.plusErrorFun(va.default)
      va.name to pf
    }.toMap()

    override fun toString(): String {
      if (p2 is Empty)
        return "MergeAgg(p=$p1, schema=${p2.resultSchema}, plusFuns=$plusFuns"
      else return "MergeUnion(p1=$p1, p2=$p2, plusFuns=$plusFuns)"
    }

    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (other?.javaClass != javaClass) return false

      other as MergeUnion0

      if (p1 != other.p1) return false
      if (p2 != other.p2) return false
      if (plusFuns != other.plusFuns) return false

      return true
    }

    override fun hashCode(): Int {
      var result = p1.hashCode()
      result = 31 * result + p2.hashCode()
      result = 31 * result + plusFuns.hashCode()
      return result
    }

    companion object {
      fun MergeUnion(
          p1: TupleOp,
          p2: TupleOp,
          plusFuns0: Map<Name, PlusFun<*>>
      ) = MergeUnion0(p1,p2,plusFuns0)

      fun MergeAgg(
          p: TupleOp,
          keysKept: Iterable<Name>,
          plusFuns0: Map<Name, PlusFun<*>>
      ) = MergeUnion0(p,
          p2 = Empty(Schema(p.resultSchema.keys.filter { it.name in keysKept }, listOf())),
          plusFuns0 = plusFuns0)



      /**
       * If A has access path (c,a) and B has access path (c,b),
       * then MergeUnion(A,B) has access path (c).
       */
      private fun intersectKeys(a: List<Attribute<*>>, b: List<Attribute<*>>): List<Attribute<*>> {
        var i = 0
        val minSize = Math.min(a.size,b.size)
        val c: MutableList<Attribute<*>> = ArrayList(minSize)
        while (i < minSize && a[i].name == b[i].name) {
          require(a[i] == b[i]) {"MergeUnion: matching keys ${a[i].name} has different types in parents: ${a[i].type} and ${b[i].type}"}
          c += a[i]
          i++
        }
        // make sure no more keys match
        require((a.subList(i,a.size) + b.subList(i,b.size)).map(Attribute<*>::name).let { it.size == it.toSet().size })
        {"MergeUnion: key attributes $a and $b have matching keys that are not in their common prefix"}
        return c
      }
      /**
       * Union maps by key. Check that entries with the same key have the same value.
       */
      private fun unionValues(a: List<ValAttribute<*>>, b: List<ValAttribute<*>>): List<ValAttribute<*>> {
        return a + b.filter { bv ->
          val av = a.find { it.name == bv.name }
          if (av != null) {
            require(av == bv) // calls equals() method
            {"MergeUnion: value attributes $a and $b have an attribute with the same name but different types"}
            false
          } else true
        }
      }
      private fun seekTransform(pKeysNotInIntersection: List<Attribute<*>>): (TupleSeekKey) -> TupleSeekKey {
        val dropKeys = pKeysNotInIntersection
        return { seek:TupleSeekKey ->
          // add new keys that are present in i1 but not in result
          // For LOWER: if OPEN, add max keys; if CLOSED, add min keys
          // For UPPER: if OPEN, add min keys; if CLOSED, add max keys
          // (OPEN is a more restrictive interval; CLOSED is larger interval)
          val lowerMin = !seek.range.hasLowerBound() || seek.range.lowerType == BoundType.CLOSED
          val upperMax = !seek.range.hasUpperBound() || seek.range.upperType == BoundType.CLOSED
          if (dropKeys.isEmpty()) seek else {
            val fLower: (Tuple) -> Tuple = { it + dropKeys.map { it.name to if (lowerMin) it.type.MIN_VALUE else it.type.MAX_VALUE } }
            val fUpper: (Tuple) -> Tuple = { it + dropKeys.map { it.name to if (!upperMax) it.type.MIN_VALUE else it.type.MAX_VALUE } }
            seek.copy(range = seek.range.transformLowerUpper(fLower, fUpper))
          }
        }
      }

    }

    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator {
      val st1 = p1.resultSchema.keys.filter { it !in resultSchema.keys }.run(MergeUnion0.Companion::seekTransform)
      val st2 = p2.resultSchema.keys.filter { it !in resultSchema.keys }.run(MergeUnion0.Companion::seekTransform)
      return MergeUnionIterator(st1, st2, resultSchema.keys,
          p1.run(instMap, env), p2.run(instMap, env), plusFuns)
    }

    class MergeUnionIterator(
        val seekTransform1: (TupleSeekKey) -> TupleSeekKey,
        val seekTransform2: (TupleSeekKey) -> TupleSeekKey,
        val keys: List<Attribute<*>>,
        val i1: TupleIterator,
        val i2: TupleIterator,
        val plusFuns: Map<Name, PlusFun<*>>
    ) : TupleIterator {
      val comparator = KeyComparator(keys)
      //      val keysAndValues = keys.map { it.name } + plusFuns.keys
      val keyNames = keys.map { it.name }
      //      var old: NameTuple = keys.map { it.name to it.type.examples.first() }.toMap()
      private var top: Tuple? by Staged { findTop() }

      override fun deepCopy(env: IteratorEnvironment) = MergeUnionIterator(seekTransform1, seekTransform2, keys, i1.deepCopy(env), i2.deepCopy(env), plusFuns)


      override fun seek(seek: TupleSeekKey) {
        i1.seek(seekTransform1(seek))
        i2.seek(seekTransform2(seek))
        top = findTop()
      }

      override fun hasNext() = top != null
      override fun next(): Tuple {
        val t = top ?: throw NoSuchElementException()
        top = findTop()
        return t
      }
      override fun peek(): Tuple = top ?: throw NoSuchElementException()

      fun getCompare(): Int = when {
        i1.hasNext() && i2.hasNext() -> comparator.compare(i1.peek(), i2.peek())
        i1.hasNext() -> -1
        i2.hasNext() -> 1
        else -> throw NoSuchElementException()
      }.let { Integer.signum(it) }

      fun findTop(): Tuple? {
        if (!(i1.hasNext() || i2.hasNext())) return null

        var c = getCompare()
        val old = if (c == 1) i2.peek() else i1.peek()
        var cur: Tuple

        // first iteration: set result to the values from i1 or i2
        var result = when (c) {
          -1 -> putDefault(i1.next())
          1 -> putDefault(i2.next())
          else -> addValues(i1.next(), i2.next())
        }

        if (i1.hasNext() || i2.hasNext()) {
          c = getCompare()
          cur = if (c == 1) i2.peek() else i1.peek()

          while (comparator.compare(old, cur) == 0) {
            // add the current matching values into the result
            result = addValues(result,
                when (c) {
                  -1 -> putDefault(i1.next())
                  1 -> putDefault(i2.next())
                  else -> addValues(i1.next(), i2.next())
                })
            if (!(i1.hasNext() || i2.hasNext())) break
            c = getCompare()
            cur = if (c == 1) i2.peek() else i1.peek()
          }
        }
        return result + old.filterKeys { it in keyNames }
      }

      private fun putDefault(t: Tuple): Tuple {
        return plusFuns.mapValues { (name,f) ->
          if (name in t) t[name]!!
          else f.identity
        }
      }

      private fun addValues(t1: Tuple, t2: Tuple): Tuple {
        return plusFuns.mapValues { (name,f) ->
          @Suppress("UNCHECKED_CAST")
          when {
            name in t1 && name in t2 -> (f.plus as (Any?, Any?) -> Any?)(t1[name], t2[name])
            name in t1 -> t1[name]
            name in t2 -> t2[name]
            else -> f.identity
          }
        }
      }
    }



  }

  fun rename(renameMap: Map<Name,Name>) = Rename(this, renameMap)
  data class Rename(
      val p: TupleOp,
      val renameMap: Map<Name,Name>
  ) : TupleOp(p) {
    override val resultSchema = p.resultSchema.let { Schema(
        it.keys.map { attr -> renameMap[attr.name]?.let { attr.withNewName(it) } ?: attr },
        it.vals.map { attr -> renameMap[attr.name]?.let { attr.withNewName(it) } ?: attr }
    ) }
    override fun reconstruct(args: Array<TupleOp>) = if (args[0] === p) this else Rename(args[0], renameMap)
    fun reconstruct(p: TupleOp) = if (p === this.p) this else Rename(p, renameMap)

    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment) =
        RenameIterator(p.run(instMap, env), renameMap)

    class RenameIterator(val parentIter: TupleIterator, val renameMap: Map<Name, Name>) : TupleIterator {
      var top: Tuple? by Staged { findTop() }

      val inverseMap: Map<String, String> = renameMap.map { (k,v) -> v to k }.toMap()

      fun findTop(): Tuple? {
        return if (parentIter.hasNext()) parentIter.peek().mapKeys { (k,_) ->
          if (k in renameMap) renameMap[k]!! else k
        } else null
      }

      override fun hasNext(): Boolean = top != null

      override fun seek(seek: TupleSeekKey) {
        seek.range.transform { it.mapKeys { inverseMap[it.key] ?: it.key } }
        parentIter.seek(seek)
        top = findTop()
      }

      override fun next(): Tuple {
        val t = top ?: throw NoSuchElementException()
        parentIter.next()
        top = findTop()
        return t
      }
      override fun peek(): Tuple = top ?: throw NoSuchElementException()
      override fun deepCopy(env: IteratorEnvironment) = RenameIterator(parentIter.deepCopy(env), renameMap)
    }

  }

  fun sort(newSort: List<Name>) = Sort(this, newSort)
  //  fun sort(vararg newSort: Name): TupleOp = Sort(this, newSort.toList())
  data class Sort(
      val p: TupleOp,
      val newSort: List<Name>,
      /** Determines if this Sort actually collects all data and re-sorts or not. */
      val fullResort: Boolean = true
  ) : TupleOp(p) {
    override val resultSchema = Schema(
        newSort.apply { require(this.toSet() == p.resultSchema.keys.map { it.name }.toSet()) {"not all names re-sorted: $newSort on ${p.resultSchema}"} }
            .map { name -> p.resultSchema.keys.find{it.name == name}!! },
        p.resultSchema.vals
    )
    override fun reconstruct(args: Array<TupleOp>) = if (args[0] === p) this else copy(args[0])
    fun reconstruct(p: TupleOp) = if (p === this.p) this else copy(p)

    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator {
      return if (fullResort) {
        val l: MutableList<Tuple> = ArrayList()
        p.run(instMap, env).forEach { l += it }
        TupleIterator.DataTupleIterator(resultSchema, l)
      } else p.run(instMap, env)
    }
  }


  fun store(table: Table) = Store(this, table)
  data class Store(
      val p: TupleOp,
      val table: Table
  ) : TupleOp(p) {
    override val resultSchema = p.resultSchema
    override fun reconstruct(args: Array<TupleOp>) = if (args[0] === p) this else copy(args[0])
    fun reconstruct(p: TupleOp) = if (p === this.p) this else copy(p)
    // RWI only works at the end of a pipeline
    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment) = throw UnsupportedOperationException()
  }




  fun join(p2: TupleOp, timesFuns: Map<Name,TimesFun<*,*,*>>) = MergeJoin(this, p2, timesFuns)
  data class MergeJoin(
      val p1: TupleOp,
      val p2: TupleOp,
      val timesFuns: Map<Name,TimesFun<*,*,*>>
  ): TupleOp(p1, p2) {
    override val resultSchema = Schema(
        keys = unionKeys(p1.resultSchema.keys,p2.resultSchema.keys),
        vals = intersectValues(p1.resultSchema.vals,p2.resultSchema.vals, timesFuns)
    )
    override fun reconstruct(args: Array<TupleOp>) = if (args[0] === p1 && args[1] === p2) this else copy(args[0], args[1])
    fun reconstruct(p1: TupleOp, p2: TupleOp) = if (p1 === this.p1 && p2 === this.p2) this else copy(p1, p2)

    companion object {
      // similar to unionValues() in MergeUnion
      private fun unionKeys(a: List<Attribute<*>>, b: List<Attribute<*>>): List<Attribute<*>> {
        val commonIdxs = ArrayList<Int>(Math.min(a.size,b.size))
        val r = a + b.filter { bv ->
          val avidx = a.indexOfFirst { it.name == bv.name }
          if (avidx != -1) {
            require(a[avidx] == bv) // calls equals() method
            {"MergeJoin: key attributes $a and $b have an attribute with the same name but different types"}
            commonIdxs.add(avidx)
            false
          } else true
        }
        commonIdxs.sort()
        val x = Array(commonIdxs.size) {it}.toList()
        require(commonIdxs == x) {"some common key attributes of this MergeJoin are not in the prefix: $commonIdxs, $x, $a, $b"}
        return r
      }

      private fun intersectValues(a: List<ValAttribute<*>>, b: List<ValAttribute<*>>,
                                  timesFuns: Map<Name, TimesFun<*, *, *>>): List<ValAttribute<*>> {
        val res = a.filter { attr -> b.any { it.name == attr.name } }
            .map { attr ->
              require(attr.name in timesFuns) {"no times operator for matching value attributes $attr"}
              val battr = b.find { it.name == attr.name }!!
              val times: TimesFun<*, *, *> = timesFuns[attr.name]!!
              require(attr.default == times.leftAnnihilator)
              {"for attribute ${attr.name}, left default value ${attr.default} != times fun left annihilator ${times.leftAnnihilator}"}
              require(battr.default == times.rightAnnihilator)
              {"for attribute ${attr.name}, right default value ${battr.default} != times fun right annihilator ${times.rightAnnihilator}. LeftAttr $a, RightAttr $b, timesFun $times"}
//              ValAttribute(attr.name, times.resultType, times.resultZero)
              multiplyTypeGet(attr.name, times)
            }
        require(timesFuns.size == res.size) {"mismatched number of times functions provided, $timesFuns for result value attributes $res"}
        return res
      }
      private fun <T1,T2,T3> multiplyTypeGet(name: Name, times: TimesFun<T1,T2,T3>) = ValAttribute<T3>(
          name,
          times.resultType,
          times.resultZero
      )

      private fun seekTransform(pKeysNotInOther: List<Name>): (TupleSeekKey) -> TupleSeekKey {
        val dropKeys = pKeysNotInOther
        return { seek:TupleSeekKey ->
          seek.copy(seek.range.transform { it.filterKeys { k -> k !in dropKeys } })
        }
      }
    }

    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator {
      val p1kn = p1.resultSchema.keys.map(Attribute<*>::name)
      val p2kn = p2.resultSchema.keys.map(Attribute<*>::name)
      val p1KeysNotIn2 = p1kn.filter { it !in p2kn }
      val p2KeysNotIn1 = p2kn.filter { it !in p1kn }
      return MergeJoinIterator(seekTransform(p2KeysNotIn1), seekTransform(p1KeysNotIn2),
          p1.resultSchema.keys.intersect(p2.resultSchema.keys).toList(),
          p1.resultSchema.keys.map { it.name }, p2.resultSchema.keys.map { it.name },
          p1.run(instMap, env), p2.run(instMap, env), timesFuns)
    }

    data class MergeJoinIterator(
        val seekTransform1: (TupleSeekKey) -> TupleSeekKey,
        val seekTransform2: (TupleSeekKey) -> TupleSeekKey,
        val keys: List<Attribute<*>>, // common keys
        val p1keys: List<Name>,
        val p2keys: List<Name>,
        val i1: TupleIterator,
        val i2: TupleIterator,
        val timesFuns: Map<Name, TimesFun<*,*,*>>
    ) : TupleIterator {

      val comparator = KeyComparator(keys)
      var topIter: PeekingIterator<Tuple> by Staged { findTop() }
      var seekKey = TupleSeekKey(MyRange.all(), listOf(), false)

      override fun deepCopy(env: IteratorEnvironment) = MergeJoinIterator(seekTransform1, seekTransform2, keys, p1keys, p2keys, i1.deepCopy(env), i2.deepCopy(env), timesFuns)

      class OneRowIterator<T>(val rowComparator: Comparator<T>,
                              private val iter: PeekingIterator<T>) : PeekingIterator<T> by iter {
        val firstTuple: T? = if (iter.hasNext()) iter.peek() else null

        override fun next(): T = if (hasNext()) iter.next() else throw NoSuchElementException("the iterator is past the original row $firstTuple")

        override fun hasNext(): Boolean = iter.hasNext() && rowComparator.compare(firstTuple, iter.peek()) == 0

        override fun peek(): T = if (hasNext()) iter.peek() else throw NoSuchElementException("the iterator is past the original row $firstTuple")
      }
      fun readRow(
          /** See TupleComparatorByKeyPrefix */
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

      fun findTop(): PeekingIterator<Tuple> {
        var iter: Iterator<Tuple>
        do {
          loop@ while (i1.hasNext() && i2.hasNext()) {
            val c = comparator.compare(i1.peek(), i2.peek())
            val b = when (Integer.signum(c)) {
              -1 -> skipUntil(i1, i2.peek(), seekTransform1)
              1 -> skipUntil(i2, i1.peek(), seekTransform2)
              else -> break@loop
            }
            if (!b) return TupleIterator.EMPTY // no more matches
          }
          if (!i1.hasNext() || !i2.hasNext()) return TupleIterator.EMPTY
          // We are either aligned or out of data on at least one iterator
          val one1 = OneRowIterator(comparator, i1)
          val one2 = readRow(comparator, i2)
          iter = CartesianIterator(one1, one2, this::times) // must have at least one entry, but maybe it is the default entry
        } while (!iter.hasNext())
        return iter.peeking()
      }

      private fun skipUntil(iter: TupleIterator, toSkipTo: Tuple, seekTransform: (TupleSeekKey) -> TupleSeekKey): Boolean {
        var cnt = 0
        while (cnt < 10 && iter.hasNext() && comparator.compare(iter.peek(), toSkipTo) < 0) {
          iter.next()
          cnt++
        }
        if (iter.hasNext() && comparator.compare(iter.peek(), toSkipTo) < 0) {
          val skipRange: MyRange<Tuple> = seekKey.range.intersection(comparator, MyRange.atLeast(toSkipTo)) ?: return false
          iter.seek(seekTransform(seekKey.copy(range = skipRange)))
        }
        return iter.hasNext()
      }

      override fun seek(seek: TupleSeekKey) {
        seekKey = seek
        i1.seek(seekTransform1(seek))
        i2.seek(seekTransform2(seek))
        topIter = findTop()
      }

      override fun hasNext(): Boolean = topIter.hasNext()
      override fun peek(): Tuple = topIter.peek()
      override fun next(): Tuple {
        val r: Tuple = topIter.next()
        if (!topIter.hasNext())
          topIter = findTop()
        return r
      }

      private fun times(t1: Tuple, t2: Tuple): Tuple {
        return timesFuns.mapValues { (name,f) ->
          @Suppress("UNCHECKED_CAST")
          when {
            name in t1 && name in t2 -> (f.times as (Any?, Any?) -> Any?)(t1[name], t2[name]) // we should always have this case
            name in t1 -> t1[name]
            name in t2 -> t2[name]
            else -> f.resultZero
          }
        } + t1.filterKeys { it in p1keys } + t2.filterKeys { it in p2keys }
      }

      class CartesianIterator(
          private val firstIter: PeekingIterator<Tuple>,
          private val secondIterable: Iterable<Tuple>,
          private val multiplyOp: (Tuple, Tuple) -> Tuple
      ) : Iterator<Tuple> {
        private var secondIter: PeekingIterator<Tuple> = Iterators.peekingIterator(secondIterable.iterator())

        init {
          if (!firstIter.hasNext() || !secondIter.hasNext()) {
            while (firstIter.hasNext()) firstIter.next()
          }
        }

        /*
        1. scan left until we find a position where hasNext() is true. If all are false then terminate.
        2. advance that iterator at the position and fill in curTuples
        3. reset all iterators to the right and fill in curTuples
         */

        override fun hasNext(): Boolean = firstIter.hasNext() && secondIter.hasNext()

        override fun next(): Tuple {
          val ret = multiplyOp(firstIter.peek(), secondIter.next())
          prepNext()
          return ret
        }

        private fun prepNext() {
          if (!secondIter.hasNext()) {
            firstIter.next()
            if (!firstIter.hasNext())
              return
            secondIter = Iterators.peekingIterator(secondIterable.iterator())
          }
        }
      }


    }
  }


  data class LoadData(
      override val resultSchema: Schema,
      val iter: Iterable<Tuple>
  ) : TupleOp() {
    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment) =
        TupleIterator.DataTupleIterator(resultSchema, iter)
    override fun reconstruct(args: Array<TupleOp>) = this
    override fun toString(): String = "LoadData(resultSchema=$resultSchema)"
  }

  data class LoadOnce(
      override val resultSchema: Schema,
      private val iter: Iterator<Tuple>
  ) : TupleOp() {
    override fun reconstruct(args: Array<TupleOp>) = this
    private var ran = false
    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment): TupleIterator {
      if (ran) logger.warn{"$this ran more than once"}
      ran = true
      return TupleIterator.DataTupleIteratorOnce(resultSchema, iter.peeking())
    }
    override fun toString() = "LoadOnce(resultSchema=$resultSchema, ran=$ran)"

    companion object : Loggable {
      override val logger: Logger = logger<LoadOnce>()
    }
  }

  data class LoadTupleIterator(
      override val resultSchema: Schema,
      private val iter: TupleIterator
  ) : TupleOp() {
    override fun reconstruct(args: Array<TupleOp>) = this
    override fun _run(instMap: MutableMap<TupleOp, TupleIterator>, env: IteratorEnvironment) = iter

    companion object : Loggable {
      override val logger: Logger = logger<LoadOnce>()
    }
  }



//  data class DeepCopy(
//      val p: TupleOp
//  ) : TupleOp(p) {
//    override val resultSchema = p.resultSchema
//    override fun reconstruct(args: Array<TupleOp>) = if (args[0] === p) this else DeepCopy(args[0])
//    override fun run(): TupleIterator = DeepCopyIterator(p.run())
//
//    class DeepCopyIterator(val piter: TupleIterator) : TupleIterator {
//      override fun init(options: Map<String,String>, env: IteratorEnvironment) = piter.deepCopy(env)
//      override fun hasNext() = throw NOPE("Call init() first to DeepCopy")
//      override fun seek(seek: TupleSeekKey) = throw NOPE("Call init() first to DeepCopy")
//      override fun next() = throw NOPE("Call init() first to DeepCopy")
//      override fun peek() = throw NOPE("Call init() first to DeepCopy")
//      override fun deepCopy(env: IteratorEnvironment) = throw NOPE("Call init() first to DeepCopy")
//    }
//  }

  // todo - make a PrintTupleOp for debugging purposes
  // then can run it ClientSide if we like, after adapting KeyValues to Tuples :)


}
