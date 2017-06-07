package edu.washington.cs.laragraphulo.examples.tricount

import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.iterators.IteratorUtil
import java.util.*
import kotlin.coroutines.experimental.EmptyCoroutineContext.plus

object TriCountQuery {

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
  val lowerTriangleFilter = FilterFun("lowerTriangleFilter") { (it["r"] as Int) > (it["c"] as Int) }
  val twoTimes = TimesFun("twoTimes", 0, 0, PType.INT) {a,b -> if (a != 0 && b != 0) 2 else 0}
  val plus = PlusFun("plus", 0, Int::plus)
  val oddFilter = FilterFun("oddFilter") { it["v"] as Int % 2 == 1 }
  val decDiv2 = MapFun("decDiv2", listOf(attrV)) {
    mapOf("v" to (it["v"] as Int - 1) / 2)
  }

  // ============= QUERY



  /**
   * Require [tableR] must not exist.
   */
  fun triCount(ac: AccumuloConfig, table0: Table): Int {
    val table1 = "temp_$table0"
    GraphuloUtil.deleteTables(ac.connector, table1)
    ac.clone(table0, table1)
    val loadA = TupleOp.Load(table0, initialSchema)

    TupleOpSetting(loadA.filter(lowerTriangleFilter), table0, ac)
        .attachIterator(21, "LowerTriangleFilter")
    TupleOpSetting(TupleOp.Load(table1, initialSchema).filter(lowerTriangleFilter), table1, ac)
        .attachIterator(1, "LowerTriangleFilter")

    val query = loadA.rename(mapOf("r" to "m", "c" to "r"))
        .join(loadA.rename(mapOf("r" to "m")), mapOf("v" to twoTimes), lowerTriangleFilter)
        .sort(listOf("r","c"), droppedKeys = listOf("m"))
        .store(table1, aggMap = mapOf("v" to plus))
    TupleOpSetting(query, table0, ac).executeSingleStore()

//    GraphuloUtil.printTable(ac.connector, table1, initialSchema.defaultPSchema())

    val query2 = TupleOp.Load(table1, initialSchema)
        .filter(oddFilter)
        .map(decDiv2)
        .agg(listOf(), mapOf("v" to plus))

    return TupleOpSetting(query2, table1, ac).executeCustom(19) { iter ->
      check(iter.hasNext()) {"Did not receive triangle count"}
      val next = iter.next()
      attrV.type.defaultPhysical.decode(next.value.get())
    }
  }



  // ============= EXPECTED RESULT
  const val expectedTriCount = 2
}