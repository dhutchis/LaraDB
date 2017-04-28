package edu.washington.cs.laragraphulo.api

/**
 * Example queries given with the NameSchema Lara API.
 */

// =========== ATTRIBUTEs
val LongAttr = AttributeImpl(Long::class.java)
val StringAttr = AttributeImpl(String::class.java)
val DoubleNullAttr = ValAttributeImpl(Double::class.java, null)
val DoubleZeroAttr = ValAttributeImpl(Double::class.java, 0.0)
val IntZeroAttr = ValAttributeImpl(Int::class.java, 0)

val nullTuple: NameTuple = mapOf("v" to null)
val zeroIntTuple: NameTuple = mapOf("v" to 0)

val attrT = "t" to LongAttr
val attrC = "c" to StringAttr
val attrTp = "t'" to LongAttr
val attrVn = "v" to DoubleNullAttr
val attrV0 = "v" to IntZeroAttr
val attrV00 = "v" to DoubleZeroAttr
val attrCnt = "cnt" to IntZeroAttr

// =========== SCHEMAs
// need to add access path?
val schamaTCVn = NameSchemaImpl(
    keyTypes = mapOf(attrT, attrC),
    valTypes = mapOf(attrVn)
)
val schemaVn = NameSchemaImpl(
    keyTypes = mapOf(),
    valTypes = mapOf(attrVn)
)
val schemaV0 = NameSchemaImpl(
    keyTypes = mapOf(),
    valTypes = mapOf(attrV0)
)
val schemaTpVn = NameSchemaImpl(
    keyTypes = mapOf(attrTp),
    valTypes = mapOf(attrVn)
)
val schemaVnCnt = NameSchemaImpl(
    keyTypes = mapOf(),
    valTypes = mapOf(attrVn, attrCnt)
)


// =========== UDFs
const val MIN_TIME = 0L
const val MAX_TIME = Long.MAX_VALUE
const val BIN_SIZE = 60

val filterFun = NameMapFunImpl(schema = schemaVn) { tuple ->
  if (tuple["t"] as Long in MIN_TIME..MAX_TIME) tuple else nullTuple
}

val binFun = NameExtFunImpl(schema = schemaTpVn) { tuple ->
  val t = tuple["t"] as Long
  val tm = t % BIN_SIZE
  val tb = t - tm + (if (tm >= BIN_SIZE /2) BIN_SIZE else 0) // new t
  val res: NameTuple = mapOf("t'" to tb, "v" to tuple["v"])
  listOf(res)
}

val createCntFun = NameMapFunImpl(schema = schemaVnCnt) { tuple ->
  tuple + ("cnt" to if (tuple["v"] != null) 1 else 0)
}
val divideVnCntFun = NameMapFunImpl(schema = schemaVn) { tuple ->
  val v = tuple["v"]
  val res = if (v != null) v as Double / tuple["cnt"] as Int else null
  tuple - "cnt" + ("v" to res)
}

val subtractVn = NameTimesFunImpl<Double?,Double?,Double?>(null, null) { a, b ->
  if (a != null && b != null) a - b else null
}
val multiplyVn = NameTimesFunImpl<Double?,Double?,Double?>(null, null) { a, b ->
  if (a != null && b != null) a * b else null
}
val divideMinusOneFun = NameTimesFunImpl<Double?,Int,Double?>(null, 0) { a, b ->
  if (a != null && b != 0) a / (b - 1) else null
}

val notNullFun = NameMapFunImpl(schema = schemaV0) { tuple ->
  if (tuple["v"] != null) tuple else zeroIntTuple
}
val anyFun = NamePlusFunImpl(0) { a, b -> if (a != 0 || b != 0) 1 else 0 }
val plusIntFun = NamePlusFunImpl(0, Int::plus)
val plusDoubleNullFun = NamePlusFunImpl<Double?>(null) { a, b ->
  when {
    a == null -> b
    b == null -> a
    else -> a + b
  }
}


// =========== QUERY
val X = listOf(
    NameLoad("tableA", schamaTCVn),
    NameLoad("tableB", schamaTCVn)
).map { NameExt(it, filterFun.asNameExtFun()) }
    .map { NameExt(it, binFun) }
    .map { NameExt(it, createCntFun.asNameExtFun()) }
    .map { NameMergeAgg(it, setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) }
    .map { NameExt(it, divideVnCntFun.asNameExtFun()) }
    .run { NameMergeJoin(this[0], this[1], mapOf("v" to subtractVn)) }

val N = NameExt(X, notNullFun.asNameExtFun())
    .run { NameMergeAgg(this, setOf("t'"), mapOf("v" to anyFun)) }
    .run { NameMergeAgg(this, setOf(), mapOf("v" to plusIntFun)) }

val M = NameExt(X, createCntFun.asNameExtFun())
    .run { NameMergeAgg(this, setOf("c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) }
    .run { NameExt(this, divideVnCntFun.asNameExtFun()) }

val U = NameMergeJoin(X, M, mapOf("v" to subtractVn))

val C = NameMergeJoin(U, NameRename(U, mapOf("c" to "c'")), mapOf("v" to multiplyVn))
    .run { NameMergeAgg(this, setOf("c", "c'"), mapOf("v" to plusDoubleNullFun)) }
    .run { NameMergeJoin(this, N, mapOf("v" to divideMinusOneFun)) }

