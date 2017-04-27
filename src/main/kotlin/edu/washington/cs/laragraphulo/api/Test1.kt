package edu.washington.cs.laragraphulo.api

/**
 * Example queries given with the NameSchema Lara API.
 */

// =========== ATTRIBUTEs
val LongAttr = AttributeImpl(Long::class.java)
val StringAttr = AttributeImpl(String::class.java)
val DoubleNullAttr = ValAttributeImpl(Double::class.java, null)
val DoubleZeroAttr = ValAttributeImpl(Double::class.java, 0.0)

val nullTuple: NameTuple = mapOf("v" to null)


// =========== SCHEMAs
// need to add access path?
val schamaTCVn = NameSchemaImpl(
    keyTypes = mapOf("t" to LongAttr, "c" to StringAttr),
    valTypes = mapOf("v" to DoubleNullAttr)
)
val schemaVn = NameSchemaImpl(
    keyTypes = mapOf(),
    valTypes = mapOf("v" to DoubleNullAttr)
)
val schemaTpVn = NameSchemaImpl(
    keyTypes = mapOf("t'" to LongAttr),
    valTypes = mapOf("v" to DoubleNullAttr)
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

val multiplyVn = NameTimesFunImpl<Double?,Double?,Double?>(null, null) { a, b ->
  when {
    a == null -> b
    b == null -> a
    else -> a * b
  }
}


// =========== QUERY
val query = listOf(
    NameLoad("tableA", schamaTCVn),
    NameLoad("tableB", schamaTCVn)
).map { NameExt(it, filterFun.asNameExtFun()) }
    .map { NameExt(it, binFun) }
    .run { NameMergeJoin(this[0], this[1], mapOf("v" to multiplyVn)) }

