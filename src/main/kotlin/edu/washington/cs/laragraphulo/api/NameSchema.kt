package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.opt.Name
import org.apache.accumulo.core.client.lexicoder.Lexicoder
import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder


// ======================= HELPER FUNCTIONS

fun <E> Collection<E>.disjoint(other: Collection<E>): Boolean {
  return this.none { other.contains(it) }
}

/**
 * Return a NameTuple with the same keys but the values set to the default values.
 */
fun NameTuple.copyDefault(ns: NameSchema): NameTuple {
  require(this.keys == (ns.keys + ns.vals).toSet())
  return this.mapValues { (attr, value) ->
    ns.getValue(attr)?.default ?: value
  }
}


// ======================= ATTRIBUTES
const val ZERO_BYTE: Byte = 0
val SINGLE_ZERO = byteArrayOf(ZERO_BYTE) // sort null values first

/** Would this come in handy? Uses an extra byte to flag null values. Probably not. */
class NullLexicoder<T>(
    private val lexicoder: Lexicoder<T>
) : AbstractLexicoder<T>() {
  override fun encode(v: T): ByteArray {
    return if (v == null) {
      SINGLE_ZERO
    } else {
      val e = lexicoder.encode(v)
      val r = ByteArray(e.size+1)
      r[0] = 1
      System.arraycopy(e,0,r,1,e.size)
      r
    }
  }

  override fun decodeUnchecked(b: ByteArray, offset: Int, len: Int): T? {
    return if (b.size == 1 && b[0] == ZERO_BYTE) null
    else decodeUnchecked(b, 1, b.size-1)
  }
}

// Idea: create a Type class instead of this class approach
// See the sealed Type class in the other package
//sealed class Type<T> {
//}

open class Attribute<out T>(
    val name: Name,
    val type: Class<out T>
) : Comparable<Attribute<*>> {
//  val type = sample0 ?: throw IllegalArgumentException("passed in a null type for attribute $name")

  open fun withNewName(n: Name) = Attribute(n, type)

  override fun toString(): String {
    return "Attribute(name='$name', type=$type)"
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Attribute<*>

    if (name != other.name) return false
    if (type != other.type) return false

    return true
  }

  override fun hashCode(): Int {
    var result = name.hashCode()
    result = 31 * result + type.hashCode()
    return result
  }

  /** Careful: this returns 0 on objects that are not equal */
  override fun compareTo(other: Attribute<*>): Int = name.compareTo(other.name)
}

class ValAttribute<out T>(
    name: Name,
    type: Class<out T>,
    val default: T
) : Attribute<T>(name, type) {

  override fun withNewName(n: Name) = ValAttribute(n, type, default)

  override fun toString(): String {
    return "ValAttribute(name='$name', type=$type, default=$default)"
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    if (!super.equals(other)) return false

    other as ValAttribute<*>

    if (default != other.default) return false

    return true
  }

  override fun hashCode(): Int {
    var result = super.hashCode()
    result = 31 * result + (default?.hashCode() ?: 0)
    return result
  }
}



// ======================= SCHEMA

data class NameSchema(
  val keys: List<Attribute<*>>,
  val vals: List<ValAttribute<*>>
) {
  init {
    val kns = keys.map(Attribute<*>::name)
    val vns = vals.map(ValAttribute<*>::name)
    require(kns.let { it.size == it.toSet().size }) {"there is a duplicate key attribute name: $keys"}
    require(vns.let { it.size == it.toSet().size }) {"there is a duplicate value attribute name: $vals"}
    require(kns.disjoint(vns)) { "keys and vals overlap: $keys, $vals" }
  }

  operator fun get(n: Name): Attribute<*>? =
      keys.find { it.name == n } ?: vals.find { it.name == n }

  fun getValue(n: Name): ValAttribute<*>? =
      vals.find { it.name == n }
}

// ======================= TUPLE

typealias NameTuple = Map<Name,*>



// ======================= UDFs

open class NameExtFun(
    /** The (to be appended) new key attributes and value attributes that the extFun produces */
    val extSchema: NameSchema,
    val extFun: (NameTuple) -> List<NameTuple>
)

/**
 * Must return default values when passed default values, for any key.
 */
class NameMapFun(
    /** The value attributes that the mapFun produces */
    val mapValues: List<ValAttribute<*>>,
    val mapFun: (NameTuple) -> NameTuple
) : NameExtFun(extSchema = NameSchema(listOf(), mapValues),
               extFun = { tuple -> listOf(mapFun(tuple)) })



class PlusFun<T>(
    val identity: T,
    val plusFun: (T, T) -> T
) {
  fun verifyIdentity(a: T = identity) {
    check(plusFun(a,identity) == a && plusFun(identity,a) == a) {"Value $a violates the identity requirement of plus for identity $identity"}
  }

  companion object {
    /** Wraps a function to have an identity. */
    inline fun <T> plusWithIdentityFun(id: T, crossinline plusFun: (T,T) -> T) = PlusFun(id) { a, b ->
      when {
        a == id -> b
        b == id -> a
        else -> plusFun(a,b)
      }
    }

    /** Wraps a function to have identity null (that is zero-sum-free). */
    inline fun <T : Any> plusWithNullIdentityFun(crossinline plusFun: (T,T) -> T): PlusFun<T?> {
      return PlusFun<T?>(null) { a, b ->
        when {
          a == null -> b
          b == null -> a
          else -> plusFun(a,b)
        }
      }
    }

    /** Use this when you know that summation will never occur. Throws an error when summing two non-identities. */
    fun <T> plusErrorFun(id: T) = PlusFun(id) { a, b ->
      when {
        a == id -> b
        b == id -> a
        else -> throw IllegalStateException("no plus function defined for this attribute, yet non-identity ($id) values $a and $b are to be added")
      }
    }
  }
}



class TimesFun<T1,T2,out T3>(
    val leftAnnihilator: T1,
    val rightAnnihilator: T2,
    val timesFun: (T1, T2) -> T3
) {
  val resultZero = timesFun(leftAnnihilator, rightAnnihilator)
  fun verifyAnnihilator(a: T1 = leftAnnihilator, b: T2 = rightAnnihilator) {
    check(timesFun(a,rightAnnihilator) == resultZero && timesFun(leftAnnihilator,b) == resultZero)
    { "Value $a and $b violate the annihilator requirement of times for annihilators $leftAnnihilator and $rightAnnihilator" }
  }

  companion object {
    /** Wraps a function to have these annihilators. */
    inline fun <T1, T2, T3> timesWithAnnihilatorsFun(
        leftAnnihilator: T1, rightAnnihilator: T2,
        crossinline timesFun: (T1, T2) -> T3
    ): TimesFun<T1, T2, T3> {
      val resultZero = timesFun(leftAnnihilator, rightAnnihilator)
      return TimesFun(leftAnnihilator, rightAnnihilator) { a, b ->
        if (a == leftAnnihilator || b == rightAnnihilator) resultZero else timesFun(a, b)
      }
    }

    /** Wraps a function to have null annihilators (with zero product property). */
    inline fun <T1, T2, T3> timesWithNullAnnihilatorsFun(
        crossinline timesFun: (T1, T2) -> T3
    ): TimesFun<T1?, T2?, T3?> {
      return TimesFun<T1?, T2?, T3?>(null, null) { a, b ->
        if (a == null || b == null) null else timesFun(a, b)
      }
    }
  }
}



// ======================= OPERATORS


sealed class NameTupleOp(
    val resultSchema: NameSchema
) {
//  abstract fun run(t: Iterator<NameTuple>): Iterator<NameTuple>

  data class Ext(
      val parent: NameTupleOp,
      /** This can also be a [NameMapFun] */
      val extFun: NameExtFun
  ): NameTupleOp(NameSchema(
      keys = parent.resultSchema.keys + extFun.extSchema.keys,
      vals = extFun.extSchema.vals
  ))

  data class Load(
      val table: String,
      val schema: NameSchema
  ): NameTupleOp(schema)

  data class Empty(
      val schema: NameSchema
  ) : NameTupleOp(schema)


  /**
   * Restricted to two parents. Future work could extend this to any number of parents.
   */
  sealed class MergeUnion0(
      val p1: NameTupleOp,
      val p2: NameTupleOp,
      plusFuns0: Map<Name, PlusFun<*>>
  ): NameTupleOp(NameSchema(
      keys = intersectKeys(p1.resultSchema.keys,p2.resultSchema.keys),
      vals = unionValues(p1.resultSchema.vals,p2.resultSchema.vals)
  )) {
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
      return "NameMergeUnion(p1=$p1, p2=$p2, plusFuns=$plusFuns)"
    }

    override fun equals(other: Any?): Boolean {
      if (this === other) return true
      if (other?.javaClass != javaClass) return false

      other as MergeUnion

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
    }

    class MergeUnion(
        p1: NameTupleOp,
        p2: NameTupleOp,
        plusFuns0: Map<Name, PlusFun<*>>
    ) : MergeUnion0(p1,p2,plusFuns0)

    class MergeAgg(
        p: NameTupleOp,
        keysKept: Collection<Name>,
        plusFuns0: Map<Name, PlusFun<*>>
    ) : MergeUnion0(p,
        p2 = Empty(NameSchema(p.resultSchema.keys.filter { it.name in keysKept }, listOf())),
        plusFuns0 = plusFuns0)
  }

  data class NameRename(
      val p: NameTupleOp,
      val renameMap: Map<Name,Name>
  ) : NameTupleOp(p.resultSchema.let { NameSchema(
      it.keys.map { attr -> renameMap[attr.name]?.let { attr.withNewName(it) } ?: attr },
      it.vals.map { attr -> renameMap[attr.name]?.let { attr.withNewName(it) } ?: attr }
  ) })


  data class MergeJoin(
      val p1: NameTupleOp,
      val p2: NameTupleOp,
      val timesFuns: Map<Name,TimesFun<*,*,*>>
  ): NameTupleOp(NameSchema(
      keys = unionKeys(p1.resultSchema.keys,p2.resultSchema.keys),
      vals = intersectValues(p1.resultSchema.vals,p2.resultSchema.vals, timesFuns)
  )) {
    init {

    }

    companion object {
      // similar to unionValues() in MergeUnion
      private fun unionKeys(a: List<Attribute<*>>, b: List<Attribute<*>>): List<Attribute<*>> {
        return a + b.filter { bv ->
          val av = a.find { it.name == bv.name }
          if (av != null) {
            require(av == bv) // calls equals() method
            {"MergeJoin: key attributes $a and $b have an attribute with the same name but different types"}
            false
          } else true
        }
      }

      private fun intersectValues(a: List<ValAttribute<*>>, b: List<ValAttribute<*>>,
                                  timesFuns: Map<Name, TimesFun<*, *, *>>): List<ValAttribute<*>> {
        val res = a.filter { attr -> b.any { it.name == attr.name }
//          val battr = b.findLast { it.name == attr.name }
        }.map { attr ->
          require(attr.name in timesFuns) {"no times operator for matching value attributes $attr"}
          val battr = b.find { it.name == attr.name }!!
          val times = timesFuns[attr.name]!!
          require(attr.default == times.leftAnnihilator)
          {"for attribute ${attr.name}, left default value ${attr.default} != times fun left annihilator ${times.leftAnnihilator}"}
          require(battr.default == times.rightAnnihilator)
          {"for attribute ${attr.name}, right default value ${battr.default} != times fun right annihilator ${times.rightAnnihilator}"}
          multiplyTypeGet(attr.name, times)
        }
        require(timesFuns.size == res.size) {"mismatched number of times functions provided, $timesFuns for result value attributes $res"}
        return res
      }

      /* d1 and d2 must be Any?, to satisfy type-checker */
      private inline fun <T1,T2,reified T3> multiplyTypeGet(name: Name, times: TimesFun<T1,T2,T3>) = ValAttribute(
          name,
          T3::class.java,
          times.resultZero
      )

    }

  }


}




///* First lower to keep the names with the scheams. Then erase the names.
// */
//
//interface PosSchema {
//  val names: List<Name>
//  val types: List<Attribute<*>>
//}
//interface PosTuple {
//  val attrs: List<*>
//}
