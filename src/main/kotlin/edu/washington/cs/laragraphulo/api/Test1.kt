package edu.washington.cs.laragraphulo.api
import edu.washington.cs.laragraphulo.api.NameTupleOp.*
import edu.washington.cs.laragraphulo.api.NameTupleOp.MergeUnion0.*

/**
 * Example queries given with the NameSchema Lara API.
 */

// =========== ATTRIBUTEs
val attrT = Attribute("t", Long::class.java)
val attrC = Attribute("c", String::class.java)
val attrTp = Attribute("t'", Long::class.java)
val attrVn = ValAttribute("v", Double::class.java, null) // May need to revisit including type class objects, w.r.t. nullability
val attrV0 = ValAttribute("v", Int::class.java, 0) 
val attrV00 = ValAttribute("v", Double::class.java, 0.0) 
val attrCnt = ValAttribute("cnt", Int::class.java, 0)

val nullTuple: NameTuple = mapOf("v" to null)
val zeroIntTuple: NameTuple = mapOf("v" to 0)


// =========== SCHEMAs
val initialSchema = NameSchema(
    keys = listOf(attrT, attrC),
    vals = listOf(attrVn)
)


// =========== UDFs
const val MIN_TIME = 0L
const val MAX_TIME = Long.MAX_VALUE
const val BIN_SIZE = 60

// idea: remove default value from schema in value attributes; compute in Ext NameTupleOp class
val filterFun = NameMapFun(mapValues = listOf(attrVn)) { tuple ->
  if (tuple["t"] as Long in MIN_TIME..MAX_TIME) tuple else nullTuple
}

val binFun = NameExtFun(extSchema = NameSchema(listOf(attrTp), listOf(attrVn))) { tuple ->
  val t = tuple["t"] as Long
  val tm = t % BIN_SIZE
  val tb = t - tm + (if (tm >= BIN_SIZE /2) BIN_SIZE else 0) // new t
  val res: NameTuple = mapOf("t'" to tb, "v" to tuple["v"])
  listOf(res)
}

val createCntFun = NameMapFun(listOf(attrVn, attrCnt)) { tuple ->
  tuple + ("cnt" to if (tuple["v"] != null) 1 else 0)
}
val divideVnCntFun = NameMapFun(listOf(attrVn)) { tuple ->
  val v = tuple["v"]
  val res = if (v != null) v as Double / tuple["cnt"] as Int else null
  tuple - "cnt" + ("v" to res)
}

val subtractVn = TimesFun<Double?,Double?,Double?>(null, null) { a, b ->
  if (a != null && b != null) a - b else null
}
val multiplyVn = TimesFun<Double?,Double?,Double?>(null, null) { a, b ->
  if (a != null && b != null) a * b else null
}
val divideMinusOneFun = TimesFun<Double?,Int,Double?>(null, 0) { a, b ->
  if (a != null && b != 0) a / (b - 1) else null
}

val notNullFun = NameMapFun(listOf(attrV0)) { tuple ->
  if (tuple["v"] != null) tuple else zeroIntTuple
}
val anyFun = PlusFun(0) { a, b -> if (a != 0 || b != 0) 1 else 0 }
val plusIntFun = PlusFun(0, Int::plus)
val plusDoubleNullFun = PlusFun.plusWithNullIdentityFun<Double>(Double::plus)


// =========== QUERY
val X = listOf(
    Load("tableA", initialSchema),
    Load("tableB", initialSchema)
).map { Ext(it, filterFun) }
    .map { Ext(it, binFun) }
    .map { Ext(it, createCntFun) }
    .map { MergeAgg(it, setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) }
    .map { Ext(it, divideVnCntFun) }
    .run { MergeJoin(this[0], this[1], mapOf("v" to subtractVn)) }

val N = Ext(X, notNullFun)
    .run { MergeAgg(this, setOf("t'"), mapOf("v" to anyFun)) }
    .run { MergeAgg(this, setOf(), mapOf("v" to plusIntFun)) }

val M = Ext(X, createCntFun)
    .run { MergeAgg(this, setOf("c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) }
    .run { Ext(this, divideVnCntFun) }

val U = MergeJoin(X, M, mapOf("v" to subtractVn))

val C = MergeJoin(U, NameRename(U, mapOf("c" to "c'")), mapOf("v" to multiplyVn))
    .run { MergeAgg(this, setOf("c", "c'"), mapOf("v" to plusDoubleNullFun)) }
    .run { MergeJoin(this, N, mapOf("v" to divideMinusOneFun)) }

