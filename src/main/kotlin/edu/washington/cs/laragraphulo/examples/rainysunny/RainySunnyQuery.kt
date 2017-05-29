package edu.washington.cs.laragraphulo.examples.rainysunny

import edu.washington.cs.laragraphulo.api.*

object RainySunnyQuery {
  const val table1 = "testRainySunny1"

  // ============= INPUT DATA
  val rainyData = listOf(
      /* Row to Value */
      "msg1" to "Hello World!",
      "msg2" to "Hello Glorious World!",
      "msg3" to "Hello Rainy World!"
  ).map { (n,msg) -> mapOf("k" to n, "v" to msg) }

  // ============= ATTRIBUTES
  val attrK = Attribute("k", LType.STRING)
  val attrV = ValAttribute("v", LType.STRING, "")

  // ============= SCHEMA
  val initialSchema = Schema(
      keys = listOf(attrK),
      vals = listOf(attrV)
  )

  // ============= UDF
  val mapFun = MapFun("RainyToSunny", listOf(attrV)) { tuple ->
    mapOf("v" to (tuple["v"] as String).replace("Rainy", "Sunny"))
  }

  // ============= QUERY
  val query = TupleOp.Load(table1, initialSchema)
      .ext(mapFun)

  // ============= EXPECTED RESULT
  val sunnyData = rainyData.map { it.mapValues { (_,v) -> v.replace("Rainy", "Sunny") } }
}