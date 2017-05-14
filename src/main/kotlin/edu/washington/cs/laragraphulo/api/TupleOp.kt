package edu.washington.cs.laragraphulo.api

import com.google.common.collect.BoundType
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.warn
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.slf4j.Logger
import java.io.Serializable
import java.util.*

sealed class TupleOp : Serializable {
  abstract val resultSchema: Schema
  abstract fun run(): TupleIterator
  /** Transform this TupleOp stack. The [TupleOp] passed to [f] is after its parents are transformed. */
  abstract fun transform(f: (TupleOp) -> TupleOp): TupleOp
  /** Visit each op and run a function on it without altering it */
  fun visit(f: (TupleOp) -> Unit) = transform { f(it); it }
  /** Do a structural fold over this TupleOp stack. [combine] should be **commutative**. */
  inline fun <T> fold(init: T, crossinline combine: (T, T) -> T, crossinline f: (TupleOp) -> T): T {
    var t: T = init
    visit { t = combine(t, f(it)) }
    return t
  }


  fun ext(extFun: ExtFun): TupleOp = Ext(this, extFun)
  data class Ext(
      val parent: TupleOp,
      /** This can also be a [MapFun] */
      val extFun: ExtFun
  ): TupleOp() {
    override val resultSchema = Schema(
        keys = parent.resultSchema.keys + extFun.extSchema.keys,
        vals = extFun.extSchema.vals
    )
    override fun transform(f: (TupleOp) -> TupleOp) = parent.transform(f).let { if (it == parent) this else copy(it) }.run(f)
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

    override fun run(): TupleIterator = ExtIterator(parent.run(), extFun, parentKeyNames)

    class ExtIterator(
        val iter: TupleIterator,
        val extFun: ExtFun,
        val parentKeyNames: List<Name>
    ) : TupleIterator {
      private var top: PeekingIterator<NameTuple> by Staged { findTop() }

      fun findTop(): PeekingIterator<NameTuple> {
        if (!iter.hasNext())
          return Collections.emptyIterator<NameTuple>().peeking()
        var topIter: Iterator<NameTuple>
        var topParent: NameTuple
        do {
          topParent = iter.next()
          topIter = extFun.extFun(topParent).iterator()
        } while (iter.hasNext() && !topIter.hasNext())
        return PrependKeysIterator(parentKeyNames, topParent, topIter).peeking()
      }

      override fun hasNext(): Boolean = top.hasNext()
      override fun next(): NameTuple {
        val r = top.next()
        if (!top.hasNext()) top = findTop()
        return r
      }
      override fun peek(): NameTuple = top.peek()
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
        parent: NameTuple,
        val iter: Iterator<NameTuple>
    ) : Iterator<NameTuple> {
      val parentKeys = parent.filterKeys { it in keysToPrepend }
      override fun hasNext(): Boolean = iter.hasNext()
      override fun next(): NameTuple {
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
    override fun run() = throw UnsupportedOperationException("Cannot run a Load() Op; need to provide a data source for this: $this")
    override fun transform(f: (TupleOp) -> TupleOp) = f(this)
  }

  data class Empty(
      override val resultSchema: Schema
  ) : TupleOp() {
    override fun run() = TupleIterator.EMPTY
    override fun transform(f: (TupleOp) -> TupleOp) = f(this)
  }


  /**
   * Restricted to two parents. Future work could extend this to any number of parents.
   */
  sealed class MergeUnion0(
      val p1: TupleOp,
      val p2: TupleOp,
      plusFuns0: Map<Name, PlusFun<*>>
  ): TupleOp() {
    override final val resultSchema = Schema(
        keys = intersectKeys(p1.resultSchema.keys,p2.resultSchema.keys),
        vals = unionValues(p1.resultSchema.vals,p2.resultSchema.vals)
    )

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

    override fun toString(): String = "MergeUnion(p1=$p1, p2=$p2, plusFuns=$plusFuns)"

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
            val fLower: (NameTuple) -> NameTuple = { it + dropKeys.map { it.name to if (lowerMin) it.type.MIN_VALUE else it.type.MAX_VALUE } }
            val fUpper: (NameTuple) -> NameTuple = { it + dropKeys.map { it.name to if (!upperMax) it.type.MIN_VALUE else it.type.MAX_VALUE } }
            seek.copy(range = seek.range.transformLowerUpper(fLower, fUpper))
          }
        }
      }

    }

    override fun run(): TupleIterator {
      val st1 = p1.resultSchema.keys.filter { it !in resultSchema.keys }.run(MergeUnion0.Companion::seekTransform)
      val st2 = p2.resultSchema.keys.filter { it !in resultSchema.keys }.run(MergeUnion0.Companion::seekTransform)
      return MergeUnionIterator(st1, st2, resultSchema.keys, p1.run(),
          p2.run(), plusFuns)
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
      private var top: NameTuple? by Staged { findTop() }

      override fun deepCopy(env: IteratorEnvironment) = MergeUnionIterator(seekTransform1, seekTransform2, keys, i1.deepCopy(env), i2.deepCopy(env), plusFuns)


      override fun seek(seek: TupleSeekKey) {
        i1.seek(seekTransform1(seek))
        i2.seek(seekTransform2(seek))
        top = findTop()
      }

      override fun hasNext() = top != null
      override fun next(): NameTuple {
        val t = top ?: throw NoSuchElementException()
        top = findTop()
        return t
      }
      override fun peek(): NameTuple = top ?: throw NoSuchElementException()

      fun getCompare(): Int = when {
        i1.hasNext() && i2.hasNext() -> comparator.compare(i1.peek(), i2.peek())
        i1.hasNext() -> -1
        i2.hasNext() -> 1
        else -> throw NoSuchElementException()
      }.let { Integer.signum(it) }

      fun findTop(): NameTuple? {
        if (!(i1.hasNext() || i2.hasNext())) return null

        var c = getCompare()
        val old = if (c == 1) i2.peek() else i1.peek()
        var cur: NameTuple

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

      private fun putDefault(t: NameTuple): NameTuple {
        return plusFuns.mapValues { (name,f) ->
          if (name in t) t[name]!!
          else f.identity
        }
      }

      private fun addValues(t1: NameTuple, t2: NameTuple): NameTuple {
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


    class MergeUnion(
        p1: TupleOp,
        p2: TupleOp,
        plusFuns0: Map<Name, PlusFun<*>>
    ) : MergeUnion0(p1,p2,plusFuns0) {
      fun copy(p1: TupleOp = this.p1,
               p2: TupleOp = this.p2,
               plusFuns0: Map<Name, PlusFun<*>> = this.plusFuns) = MergeUnion(p1, p2, plusFuns0)
      override fun transform(f: (TupleOp) -> TupleOp): TupleOp {
        val np1 = p1.transform(f)
        val np2 = p2.transform(f)
        return f(if (np1 == p1 && np2 == p2) this else MergeUnion(np1, np2, plusFuns))
      }
    }

    class MergeAgg(
        p: TupleOp,
        val keysKept: Collection<Name>,
        plusFuns0: Map<Name, PlusFun<*>>
    ) : MergeUnion0(p,
        p2 = Empty(Schema(p.resultSchema.keys.filter { it.name in keysKept }, listOf())),
        plusFuns0 = plusFuns0) {
      override fun transform(f: (TupleOp) -> TupleOp) = p1.transform(f).let { if (it == p1) this else MergeAgg(it, keysKept, plusFuns) }.run(f)
      override fun toString(): String = "MergeAgg(p=$p1, keysKept=$keysKept, plusFuns=$plusFuns)"
    }
  }
  fun union(p2: TupleOp, plusFuns0: Map<Name, PlusFun<*>>) = when (p2) {
    is Empty -> MergeUnion0.MergeAgg(this, p2.resultSchema.keys.map { it.name }, plusFuns0) // optimization when unioning with empty table
    else -> MergeUnion0.MergeUnion(this, p2, plusFuns0)
  }
  fun agg(keysKept: Collection<Name>, plusFuns0: Map<Name, PlusFun<*>>) = MergeUnion0.MergeAgg(this, keysKept, plusFuns0)

  fun rename(renameMap: Map<Name,Name>): TupleOp = Rename(this, renameMap)
  data class Rename(
      val p: TupleOp,
      val renameMap: Map<Name,Name>
  ) : TupleOp() {
    override val resultSchema = p.resultSchema.let { Schema(
        it.keys.map { attr -> renameMap[attr.name]?.let { attr.withNewName(it) } ?: attr },
        it.vals.map { attr -> renameMap[attr.name]?.let { attr.withNewName(it) } ?: attr }
    ) }

    override fun transform(f: (TupleOp) -> TupleOp) = p.transform(f).let { if (it == p) this else copy(it) }.run(f)

    override fun run() = RenameIterator(p.run(), renameMap)

    class RenameIterator(val parentIter: TupleIterator, val renameMap: Map<Name, Name>) : TupleIterator {
      var top: NameTuple? by Staged { findTop() }

      val inverseMap: Map<String, String> = renameMap.map { (k,v) -> v to k }.toMap()

      fun findTop(): NameTuple? {
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

      override fun next(): NameTuple {
        val t = top ?: throw NoSuchElementException()
        parentIter.next()
        top = findTop()
        return t
      }
      override fun peek(): NameTuple = top ?: throw NoSuchElementException()
      override fun deepCopy(env: IteratorEnvironment) = RenameIterator(parentIter.deepCopy(env), renameMap)
    }

  }

  fun sort(newSort: List<Name>): TupleOp = Sort(this, newSort)
  //  fun sort(vararg newSort: Name): TupleOp = Sort(this, newSort.toList())
  data class Sort(
      val p: TupleOp,
      val newSort: List<Name>
  ) : TupleOp() {
    override val resultSchema = Schema(
        newSort.apply { require(this.toSet() == p.resultSchema.keys.map { it.name }.toSet()) {"not all names re-sorted: $newSort on ${p.resultSchema}"} }
            .map { name -> p.resultSchema.keys.find{it.name == name}!! },
        p.resultSchema.vals
    )
    override fun transform(f: (TupleOp) -> TupleOp) = p.transform(f).let { if (it == p) this else copy(it) }.run(f)

    override fun run(): TupleIterator {
      val l: MutableList<NameTuple> = ArrayList()
      p.run().forEach { l += it }
      return TupleIterator.DataTupleIterator(resultSchema, l)
    }
  }

  data class Store(
      val p: TupleOp,
      val table: Table
  ) : TupleOp() {
    override val resultSchema = p.resultSchema
    override fun transform(f: (TupleOp) -> TupleOp) = p.transform(f).let { if (it == p) this else copy(it) }.run(f)
    // RWI only works at the end of a pipeline
    override fun run() = throw UnsupportedOperationException()
  }




  fun join(p2: TupleOp, timesFuns: Map<Name,TimesFun<*,*,*>>): TupleOp = MergeJoin(this, p2, timesFuns)
  data class MergeJoin(
      val p1: TupleOp,
      val p2: TupleOp,
      val timesFuns: Map<Name,TimesFun<*,*,*>>
  ): TupleOp() {
    override val resultSchema = Schema(
        keys = unionKeys(p1.resultSchema.keys,p2.resultSchema.keys),
        vals = intersectValues(p1.resultSchema.vals,p2.resultSchema.vals, timesFuns)
    )
    override fun transform(f: (TupleOp) -> TupleOp): TupleOp {
      val np1 = p1.transform(f)
      val np2 = p2.transform(f)
      return f(if (np1 == p1 && np2 == p2) this else copy(np1, np2))
    }

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

    override fun run(): TupleIterator {
      val p1kn = p1.resultSchema.keys.map(Attribute<*>::name)
      val p2kn = p2.resultSchema.keys.map(Attribute<*>::name)
      val p1KeysNotIn2 = p1kn.filter { it !in p2kn }
      val p2KeysNotIn1 = p2kn.filter { it !in p1kn }
      return MergeJoinIterator(seekTransform(p2KeysNotIn1), seekTransform(p1KeysNotIn2),
          p1.resultSchema.keys.intersect(p2.resultSchema.keys).toList(),
          p1.resultSchema.keys.map { it.name }, p2.resultSchema.keys.map { it.name },
          p1.run(), p2.run(), timesFuns)
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
      var topIter: PeekingIterator<NameTuple> by Staged { findTop() }
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
          rowComparator: Comparator<NameTuple>,
          iter: PeekingIterator<NameTuple>
      ): List<NameTuple> {
        check(iter.hasNext()) {"$iter should hasNext()"}
        val first = iter.peek()
        val list = LinkedList<NameTuple>()
        do {
          list.add(iter.next())
        } while (iter.hasNext() && rowComparator.compare(first, iter.peek()) == 0)
        return list
      }

      fun findTop(): PeekingIterator<NameTuple> {
        var iter: Iterator<NameTuple>
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

      private fun skipUntil(iter: TupleIterator, toSkipTo: NameTuple, seekTransform: (TupleSeekKey) -> TupleSeekKey): Boolean {
        var cnt = 0
        while (cnt < 10 && iter.hasNext() && comparator.compare(iter.peek(), toSkipTo) < 0) {
          iter.next()
          cnt++
        }
        if (iter.hasNext() && comparator.compare(iter.peek(), toSkipTo) < 0) {
          val skipRange: MyRange<NameTuple> = seekKey.range.intersection(comparator, MyRange.atLeast(toSkipTo)) ?: return false
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
      override fun peek(): NameTuple = topIter.peek()
      override fun next(): NameTuple {
        val r: NameTuple = topIter.next()
        if (!topIter.hasNext())
          topIter = findTop()
        return r
      }

      private fun times(t1: NameTuple, t2: NameTuple): NameTuple {
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
          private val firstIter: PeekingIterator<NameTuple>,
          private val secondIterable: Iterable<NameTuple>,
          private val multiplyOp: (NameTuple, NameTuple) -> NameTuple
      ) : Iterator<NameTuple> {
        private var secondIter: PeekingIterator<NameTuple> = Iterators.peekingIterator(secondIterable.iterator())

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

        override fun next(): NameTuple {
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
      val iter: Iterable<NameTuple>
  ) : TupleOp() {
    override fun run(): TupleIterator = TupleIterator.DataTupleIterator(resultSchema, iter)
    override fun transform(f: (TupleOp) -> TupleOp) = f(this)
    override fun toString(): String = "LoadData(resultSchema=$resultSchema)"
  }

  data class LoadOnce(
      override val resultSchema: Schema,
      private val iter: Iterator<NameTuple>
  ) : TupleOp() {
    private var ran = false
    override fun run(): TupleIterator {
      if (ran) logger.warn{"$this ran more than once"}
      ran = true
      return TupleIterator.DataTupleIteratorOnce(resultSchema, iter.peeking())
    }
    override fun transform(f: (TupleOp) -> TupleOp) = f(this)
    override fun toString(): String {
      return "LoadOnce(resultSchema=$resultSchema, ran=$ran)"
    }

    companion object : Loggable {
      override val logger: Logger = logger<LoadOnce>()
    }
  }

  data class DeepCopy(
      val p: TupleOp
  ) : TupleOp() {
    override val resultSchema = p.resultSchema
    override fun transform(f: (TupleOp) -> TupleOp) = p.transform(f).let { if (it == p) this else copy(it) }.run(f)
    override fun run(): TupleIterator = DeepCopyIterator(p.run())

    class DeepCopyIterator(val piter: TupleIterator) : TupleIterator {
      override fun init(options: Map<String,String>, env: IteratorEnvironment) = piter.deepCopy(env)
      override fun hasNext() = throw NOPE("Call init() first to DeepCopy")
      override fun seek(seek: TupleSeekKey) = throw NOPE("Call init() first to DeepCopy")
      override fun next() = throw NOPE("Call init() first to DeepCopy")
      override fun peek() = throw NOPE("Call init() first to DeepCopy")
      override fun deepCopy(env: IteratorEnvironment) = throw NOPE("Call init() first to DeepCopy")
    }
  }

}
