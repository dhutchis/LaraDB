package edu.washington.cs.laragraphulo.api



object SensorQuery {

  // =============== ATTRIBUTEs (Types are given in Types.kt)
  val attrT = Attribute("t", LType.ULONG)
  val attrC = Attribute("c", LType.STRING)
  val attrTp = Attribute("t'", LType.ULONG)
  val attrVn = ValAttribute("v", LType.NDOUBLE, null) // May need to revisit including type class objects, w.r.t. nullability
  val attrV0 = ValAttribute("v", LType.UINT, 0)
  //val attrV00 = ValAttribute("v", Double::class.java, 0.0)
  val attrCnt = ValAttribute("cnt", LType.UINT, 0)

  val nullTuple: NameTuple = mapOf("v" to null)
  val zeroIntTuple: NameTuple = mapOf("v" to 0)
  val oneIntTuple: NameTuple = mapOf("v" to 1)


  // =============== SCHEMAs
  val initialSchema = Schema(
      keys = listOf(attrT, attrC),
      vals = listOf(attrVn)
  )


  // =============== UDFs
  const val MIN_TIME = 0L
  const val MAX_TIME = Long.MAX_VALUE
  const val BIN_SIZE = 120000

  // idea: remove default value from schema in value attributes; compute in .ext TupleOp class
  val filterFun = MapFun(mapValues = listOf(attrVn)) { tuple ->
    if (tuple["t"] as Long in MIN_TIME..MAX_TIME) tuple else nullTuple
  }

  val binFun = ExtFun(extSchema = Schema(listOf(attrTp), listOf(attrVn))) { tuple ->
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

  val subtractVn = TimesFun.withNullAnnihilators<Double,Double,Double>(LType.NDOUBLE, Double::minus)
  val multiplyVn = TimesFun.withNullAnnihilators<Double,Double,Double>(LType.NDOUBLE, Double::times)
  val divideMinusOneFun = TimesFun<Double?,Int,Double?>(null, 0, LType.NDOUBLE) { a, b ->
    if (a != null && b != 0) a / (b - 1) else null
  }

  val notNullFun = MapFun(listOf(attrV0)) { tuple ->
    if (tuple["v"] != null) oneIntTuple else zeroIntTuple
  }
  val anyFun = PlusFun(0) { a, b -> if (a != 0 || b != 0) 1 else 0 }
  val plusIntFun = PlusFun(0, Int::plus)
  val plusDoubleNullFun = PlusFun.withNullIdentity<Double>(Double::plus)


  // =============== QUERY
  val A = TupleOp.Load("sensorA", initialSchema)
      .ext(filterFun)
      .ext(binFun)
      .ext(createCntFun)
      .sort(listOf("t'", "c", "t"))
      .agg(setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun))
      .ext(divideVnCntFun)

  val B = TupleOp.Load("sensorB", initialSchema)
      .ext(filterFun)
      .ext(binFun)
      .ext(createCntFun)
      .sort(listOf("t'", "c", "t"))
      .agg(setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun))
      .ext(divideVnCntFun)

  val X = A.join(B, mapOf("v" to subtractVn))

  val N = X.ext(notNullFun)
      .agg(setOf("t'"), mapOf("v" to anyFun))
      .agg(setOf(), mapOf("v" to plusIntFun))

  val X0 = X.sort(listOf("c", "t'"))

  val M = X0.ext(createCntFun)
      .agg(setOf("c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun))
      .ext(divideVnCntFun)

  val U = X0.join(M, mapOf("v" to subtractVn))
      .sort(listOf("t'","c"))

  val C = U.join(U.rename(mapOf("c" to "c'")), mapOf("v" to multiplyVn))
      .sort(listOf("c", "c'", "t'"))
      .agg(setOf("c", "c'"), mapOf("v" to plusDoubleNullFun))
      .join(N, mapOf("v" to divideMinusOneFun))

//val S = Store(C, "tableC")
}