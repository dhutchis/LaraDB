package edu.washington.cs.laragraphulo.api
import edu.washington.cs.laragraphulo.api.NameTupleOp.*
import edu.washington.cs.laragraphulo.api.LType.*
import edu.washington.cs.laragraphulo.api.NameTupleOp.MergeUnion0.*

/*
 * Example queries given with the a Lara API based on attributes-by-name.
 */


// =============== ATTRIBUTEs (Types are given in Types.kt)
val attrT = Attribute("t", ULONG)
val attrC = Attribute("c", STRING)
val attrTp = Attribute("t'", ULONG)
val attrVn = ValAttribute("v", NDOUBLE, null) // May need to revisit including type class objects, w.r.t. nullability
val attrV0 = ValAttribute("v", UINT, 0)
//val attrV00 = ValAttribute("v", Double::class.java, 0.0)
val attrCnt = ValAttribute("cnt", UINT, 0)

val nullTuple: NameTuple = mapOf("v" to null)
val zeroIntTuple: NameTuple = mapOf("v" to 0)
val oneIntTuple: NameTuple = mapOf("v" to 1)


// =============== SCHEMAs
val initialSchema = NameSchema(
    keys = listOf(attrT, attrC),
    vals = listOf(attrVn)
)


// =============== UDFs
const val MIN_TIME = 0L
const val MAX_TIME = Long.MAX_VALUE
const val BIN_SIZE = 120000

// idea: remove default value from schema in value attributes; compute in Ext NameTupleOp class
val filterFun = MapFun(mapValues = listOf(attrVn)) { tuple ->
  if (tuple["t"] as Long in MIN_TIME..MAX_TIME) tuple else nullTuple
}

val binFun = ExtFun(extSchema = NameSchema(listOf(attrTp), listOf(attrVn))) { tuple ->
  val t = tuple["t"] as Long
  val tm = t % BIN_SIZE
  val tb = t - tm + (if (tm >= BIN_SIZE /2) BIN_SIZE else 0) // new t
  val res: NameTuple = mapOf("t'" to tb, "v" to tuple["v"])
  listOf(res)
}

val createCntFun = MapFun(listOf(attrVn, attrCnt)) { tuple ->
  val v = tuple["v"]
  mapOf("v" to v, "cnt" to if (v != null) 1 else 0)
}
val divideVnCntFun = MapFun(listOf(attrVn)) { tuple ->
  val v = tuple["v"]
  val res = if (v != null) v as Double / tuple["cnt"] as Int else null
  mapOf("v" to res)
//  tuple - "cnt" + ("v" to res)
}

val subtractVn = TimesFun.withNullAnnihilators<Double,Double,Double>(NDOUBLE, Double::minus)
val multiplyVn = TimesFun.withNullAnnihilators<Double,Double,Double>(NDOUBLE, Double::times)
val divideMinusOneFun = TimesFun<Double?,Int,Double?>(null, 0, NDOUBLE) { a, b ->
  if (a != null && b != 0) a / (b - 1) else null
}

val notNullFun = MapFun(listOf(attrV0)) { tuple ->
  if (tuple["v"] != null) oneIntTuple else zeroIntTuple
}
val anyFun = PlusFun(0) { a, b -> if (a != 0 || b != 0) 1 else 0 }
val plusIntFun = PlusFun(0, Int::plus)
val plusDoubleNullFun = PlusFun.withNullIdentity<Double>(Double::plus)


// =============== QUERY
val X = listOf(
    Load("tableA", initialSchema),
    Load("tableB", initialSchema)
)
    .map { NameTupleOp.Ext(it, filterFun) }
    .map { NameTupleOp.Ext(it, binFun) }
    .map { NameTupleOp.Ext(it, createCntFun) }
    .map { NameTupleOp.Sort(it, listOf("t'", "c", "t")) }
    .map { NameTupleOp.MergeUnion0.MergeAgg(it, setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) } // fails here; need a re-sort operation
    .map { NameTupleOp.Ext(it, divideVnCntFun) }
    .run { NameTupleOp.MergeJoin(this[0], this[1], mapOf("v" to subtractVn)) }

val N = NameTupleOp.Ext(X, notNullFun)
    .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf("t'"), mapOf("v" to anyFun)) }
    .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf(), mapOf("v" to plusIntFun)) }

val X0 = NameTupleOp.Sort(X, listOf("c", "t'"))

val M = NameTupleOp.Ext(X0, createCntFun)
    .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf("c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) }
    .run { NameTupleOp.Ext(this, divideVnCntFun) }

val U = NameTupleOp.MergeJoin(X0, M, mapOf("v" to subtractVn))
    .run { NameTupleOp.Sort(this, listOf("t'","c")) }

val C = NameTupleOp.MergeJoin(U, NameTupleOp.Rename(U, mapOf("c" to "c'")), mapOf("v" to multiplyVn))
    .apply { println(this.resultSchema) }
    .apply { this.run().forEach { println(it) } }
    .run { NameTupleOp.Sort(this, listOf("c", "c'", "t'")) }
    .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf("c", "c'"), mapOf("v" to plusDoubleNullFun)) }
    .run { NameTupleOp.MergeJoin(this, N, mapOf("v" to divideMinusOneFun)) }

//val S = Store(C, "tableC")

