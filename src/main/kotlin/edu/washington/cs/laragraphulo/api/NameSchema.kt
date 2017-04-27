package edu.washington.cs.laragraphulo.api

fun <E> Collection<E>.disjoint(other: Collection<E>): Boolean {
  return this.none { other.contains(it) }
}

/**
 * Intersect maps by key. Check that entries with the same key have the same value.
 */
fun <K,V> List<Map<K,V>>.intersectAndCheckMatching(): Map<K,V> {
  val first = this.first()
  val rest = this.subList(1, this.size)
  return first.filter { (fk, fv) ->
    rest.all {
      if (it.containsKey(fk)) {
        require(it[fk] == fv) {"matching keys $fk have different types $fv, ${it[fk]}"}
        true
      } else false
    }
  }
}

/**
 * Union maps by key. Check that entries with the same key have the same value.
 */
fun <K,V> List<Map<K,V>>.unionAndCheckMatching(): Map<K,V> {
  return this.subList(1, this.size).fold(this.first()) { acc, map ->
    acc + map.filter { (k, v) ->
      if (acc.containsKey(k)) {
        require(acc[k] == v)
        false
      } else true
    }
  }
}


interface Attribute<out T> {
  val type: Class<out T>
}
interface ValAttribute<out T> : Attribute<T> {
  val default: T
}

data class AttributeImpl<out T>(
    override val type: Class<out T>
) : Attribute<T>

data class ValAttributeImpl<out T>(
    override val type: Class<out T>,
    override val default: T
) : ValAttribute<T>

///** ND for No Default */
//interface NameSchemaND {
//  val keyTypes: Map<String,Attribute<*>>
//  val valTypes: Map<String,Attribute<*>>
//}

interface NameSchema {
  val keyTypes: Map<String,Attribute<*>>
  val valTypes: Map<String,ValAttribute<*>>
  fun validate() {
    // check key and val types disjoint
    require(keyTypes.keys.disjoint(valTypes.keys)) { "keys and vals overlap: $keyTypes, $valTypes" }
  }
}

data class NameSchemaImpl(
    override val keyTypes: Map<String,Attribute<*>>,
    override val valTypes: Map<String,ValAttribute<*>>
) : NameSchema


typealias NameTuple = Map<String,*>

/**
 * Return a NameTuple with the same keys but the values set to the default values.
 */
fun NameTuple.copyDefault(ns: NameSchema): NameTuple {
  require(this.keys == (ns.keyTypes.keys + ns.valTypes.keys))
  return this.mapValues { (attr, value) ->
    ns.valTypes[attr]?.default ?: value
  }
}

interface NameTupleOp {
  val schema: NameSchema
}

interface NameExtFun : (NameTuple) -> List<NameTuple> {
  val schema: NameSchema
}

/**
 * Must return default values when passed default values, for any key.
 */
interface NameMapFun : (NameTuple) -> NameTuple {
  val schema: NameSchema

  fun asNameExtFun(): NameExtFun = object : NameExtFun {
    override fun invoke(p1: NameTuple): List<NameTuple> = listOf(this@NameMapFun(p1))
    override val schema: NameSchema = this@NameMapFun.schema
  }
}

data class NameExt(
    val parent: NameTupleOp,
    val extFun: NameExtFun
): NameTupleOp {
  override val schema: NameSchema = {
    val pk = parent.schema.keyTypes
    val s2 = extFun.schema
    require(s2.keyTypes.keys.disjoint(pk.keys)) {"keys and new keys overlap: $pk, ${s2.keyTypes}"}
    NameSchemaImpl(pk + s2.keyTypes, s2.valTypes)
  }()
}

data class NameLoad(
    val table: String,
    override val schema: NameSchema
): NameTupleOp


interface NamePlusFun<T>: (T, T) -> T {
  val identity: T

  companion object {
    fun <T> errorNamePlusFun(id: T) = object : NamePlusFun<T> {
      override val identity: T = id
      override fun invoke(p1: T, p2: T): T = when {
         p1 == identity -> p2
         p2 == identity -> p1
         else -> throw IllegalStateException("no plus function defined for this attribute, yet non-identity values $p1 and $p2 are to be added")
       }
    }
  }
}

interface NameTimesFun<T1,T2,out T3>: (T1, T2) -> T3 {
  val leftAnnihilator: T1
  val rightAnnihilator: T2
}

/**
 * Restricted to two parents. Future work could extend this to any number of parents.
 */
class NameMergeUnion(
    val p1: NameTupleOp,
    val p2: NameTupleOp,
    plusFuns0: Map<String,NamePlusFun<*>>
): NameTupleOp {
  override val schema: NameSchema =
    NameSchemaImpl(keyTypes = listOf(p1.schema.keyTypes,p2.schema.keyTypes).intersectAndCheckMatching(),
      valTypes = listOf(p1.schema.valTypes,p2.schema.valTypes).unionAndCheckMatching())
  init {
    schema.validate()
    require(schema.valTypes.keys.containsAll(plusFuns0.keys)) {"plus functions provided for values that do not exist"}
  }
  val plusFuns: Map<String,NamePlusFun<*>> = schema.valTypes.map { (k,v) ->
    k to (plusFuns0[k] ?: NamePlusFun.errorNamePlusFun(v.default))
  }.toMap()

  override fun toString(): String {
    return "NameMergeUnion(p1=$p1, p2=$p2, plusFuns=$plusFuns)"
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as NameMergeUnion

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
}

data class NameMergeJoin(
    val p1: NameTupleOp,
    val p2: NameTupleOp,
    val timesFuns: Map<String,NameTimesFun<*,*,*>>
): NameTupleOp {
  override val schema: NameSchema =
    NameSchemaImpl(keyTypes = listOf(p1.schema.keyTypes,p2.schema.keyTypes).unionAndCheckMatching(),
      valTypes = intersectAndMultiplyMatching(p1.schema.valTypes,p2.schema.valTypes))

  init {
    schema.validate()
  }

  private fun intersectAndMultiplyMatching(valTypes1: Map<String, ValAttribute<*>>, valTypes2: Map<String, ValAttribute<*>>): Map<String, ValAttribute<*>> {
    return valTypes1.filter { (k,_) -> valTypes2.containsKey(k) }.map { (k,v) ->
      require(timesFuns.containsKey(k)) {"no times operator for matching value attributes $k"}
      val times = timesFuns[k]!!
      k to (multiply(times, v.default, valTypes2[k]!!.default))
    }.toMap()
  }

  /* d1 and d2 must be Any?, to satisfy type-checker */
  private inline fun <T1,T2,reified T3> multiply(times: NameTimesFun<T1,T2,T3>, d1: Any?, d2: Any?): ValAttribute<T3> {
    require(times.leftAnnihilator == d1) {"leftAnnihilator ${times.leftAnnihilator} != d1.default $d1"}
    require(times.rightAnnihilator == d2) {"rightAnnihilator ${times.rightAnnihilator} != d2.default $d2"}
    return object : ValAttribute<T3> {
      override val type: Class<out T3> = T3::class.java
      override val default: T3 = times(times.leftAnnihilator, times.rightAnnihilator)
    }
  }
}




/* First lower to keep the names with the scheams. Then erase the names.
 */

interface PosSchema {
  val names: List<String>
  val types: List<Attribute<*>>
}
interface PosTuple {
  val attrs: List<*>
}
