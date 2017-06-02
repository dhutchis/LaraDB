package edu.washington.cs.laragraphulo.examples.ktruss

import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import edu.washington.cs.laragraphulo.util.GraphuloUtil

object KTrussQuery {
  const val table0 = "testKTruss0"
  const val table1 = "testKTruss1"
  const val table2 = "testKTruss2"
  const val k = 3

  // ============= INPUT DATA
  val exampleData = listOf(
      mapOf("r" to 1, "c" to 2, "v" to 1),
      mapOf("r" to 1, "c" to 3, "v" to 1),
      mapOf("r" to 1, "c" to 4, "v" to 1),
      mapOf("r" to 2, "c" to 3, "v" to 1),
      mapOf("r" to 3, "c" to 4, "v" to 1),

      mapOf("r" to 2, "c" to 1, "v" to 1),
      mapOf("r" to 3, "c" to 1, "v" to 1),
      mapOf("r" to 4, "c" to 1, "v" to 1),
      mapOf("r" to 3, "c" to 2, "v" to 1),
      mapOf("r" to 4, "c" to 3, "v" to 1),

      mapOf("r" to 2, "c" to 5, "v" to 1),
      mapOf("r" to 5, "c" to 2, "v" to 1)
  )

  // ============= ATTRIBUTES
  val attrR = Attribute("r", PType.INT)
  val attrC = Attribute("c", PType.INT)
  val attrV = ValAttribute("v", PType.INT, 0)

  // ============= SCHEMA
  val initialSchema = Schema(
      keys = listOf(attrR, attrC),
      vals = listOf(attrV)
  )

  // ============= UDF
//  val extFun = ExtFun("Tokenize", Schema(listOf(attrW), listOf(attrCount))) { tuple ->
//    val wordMap: Map<String, Int> = (tuple["text"] as String).split(" ")
//        .groupBy { it }.mapValues { (_,list) -> list.count() }
//    wordMap.map { (word,count) -> mapOf("word" to word, "count" to count) }
//  }
  val twoTimes = TimesFun("twoTimes", 0, 0, PType.INT) {a,b -> if (a != 0 && b != 0) 2 else 0}
  val plus = PlusFun("plus", 0, Int::plus)
  val nodiag = MapFun("nodiag", listOf(attrV)) {
    if (it["r"] == it["c"]) mapOf("v" to 0) else it
  }
  val ktrussFilter = MapFun("ktrussFilter", listOf(attrV)) {
    val v = it["v"] as Int
    if (v % 2 == 0 && (v-1)/2 >= k-2) it
    else mapOf("v" to 0)
  }
  val abs0 = MapFun("abs0", listOf(attrV)) {
    when (it["v"] as Int) {
      0, 1 -> it
      else -> mapOf("v" to 0)
    }
  }

  // ============= QUERY
  val query0 = TupleOp.Load(table0, initialSchema)
      .store(table1)

  // 1 loop iteration
  val load = TupleOp.Load(table1, initialSchema)
  val queryLoop = load.rename(mapOf("r" to "m", "c" to "r"))
      .join(load.rename(mapOf("r" to "m")), mapOf("v" to twoTimes))
      .map(nodiag)
      .sort(listOf("r", "c", "m"))
      .agg(listOf("r", "c"), mapOf("v" to plus))
      .union(load, mapOf("v" to plus))
      .map(ktrussFilter)
      .map(abs0)
      .store(table2)
  val queryCheck = queryLoop.agg(listOf(), mapOf("v" to plus))
  // check nnz between loops
  // use table2 as table1 in next loop, if there is another loop


//  // To be implemented.
//  // This is the kind of plan we need to get optimal performance through the Lara API.
//  // A complete optimizer would transform the above into this.
//  val optQuery0 = TupleOp.Clone(table0, table1)
//  val optQuery1a = TupleOp.Clone(table1, table2)
//  val optQuery1 = load.rename(mapOf("r" to "m", "c" to "r"))
//      .joinFilter(load.rename(mapOf("r" to "m")), mapOf("v" to twoTimes)) { it:Map<String,*> -> it["r"] != it["c"] }
//      .sortAgg(listOf("r", "c"), mapOf("v" to plus), table2) // store to table2
//      // somehow also count the partial products, for convergence check
//      .defer { it:TupleOp ->
//        it.map(ktrussFilter)
//            .map(abs0)
//      }
//  // switch table1, table2


//  fun kTrussAdj(ac: AccumuloConfig, tableA: Table, tableR: Table) {
//    var nppBefore: Long
//    var nppAfter = 0L
//    var totalnpp = 0L
//    var (Atmp, AtmpAlt) = "${tableA}_kTrussAdj_".let {
//      "${it}tmpA" to "${it}tmpAalt"
//    }
//    GraphuloUtil.deleteTables(ac.connector, Atmp, AtmpAlt, tableR)
//    ac.clone(tableA, Atmp)
//
//    do {
//      nppBefore = nppAfter
//
//      val load = TupleOp.Load(Atmp, initialSchema)
//      val query: TupleOp.Store = load.rename(mapOf("r" to "m", "c" to "r"))
//          .join(load.rename(mapOf("r" to "m")), mapOf("v" to twoTimes), FilterFun("nodiag") { it: Map<String, *> -> it["r"] != it["c"] })
//          .sortAgg(listOf("r", "c"), mapOf("v" to plus), AtmpAlt) // todo and store to table2
//
//      val tosQuery = TupleOpSetting(query, Atmp, ac)
//      nppAfter = tosQuery.executeSingle()
//      totalnpp += nppAfter
//
//      val filter = TupleOp.Load(AtmpAlt, initialSchema)
//          .map(ktrussFilter)
//          .map(abs0)
//      val tosFilter = TupleOpSetting(filter, AtmpAlt, ac)
//      tosFilter.attachIterator()
//
//      val t = Atmp
//      Atmp = AtmpAlt // result is in Atmp
//      AtmpAlt = t
//      GraphuloUtil.deleteTables(ac.connector, AtmpAlt)
//
//    } while (nppBefore != nppAfter)
//
//    println("total npp: $totalnpp")
//  }



  // ============= EXPECTED RESULT
  val expectedData = listOf(
      mapOf("r" to 1, "c" to 2, "v" to 1),
      mapOf("r" to 1, "c" to 3, "v" to 1),
      mapOf("r" to 1, "c" to 4, "v" to 1),
      mapOf("r" to 2, "c" to 3, "v" to 1),
      mapOf("r" to 3, "c" to 4, "v" to 1),

      mapOf("r" to 2, "c" to 1, "v" to 1),
      mapOf("r" to 3, "c" to 1, "v" to 1),
      mapOf("r" to 4, "c" to 1, "v" to 1),
      mapOf("r" to 3, "c" to 2, "v" to 1),
      mapOf("r" to 4, "c" to 3, "v" to 1)
  )
}