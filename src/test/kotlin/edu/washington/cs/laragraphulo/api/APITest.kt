package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.sensor.SensorFileAction
import edu.washington.cs.laragraphulo.api.NameTupleOp.*
import edu.washington.cs.laragraphulo.sensor.dateParser
import edu.washington.cs.laragraphulo.sensor.dateParserNoMilli
import org.apache.accumulo.core.data.Mutation
import org.junit.Test
import java.io.File
import java.net.URL
import java.util.*

/** Data for the first sensor */
const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"

class APITest  { // : AccumuloTestBase()


/*
 * First, load some data.
 */

//  val rowTransform: (String) -> List<Mutation> = { line ->
//    val parts = line.split(';')
//    if (parts.size < 6) return@forEach
//    if (parts[2] == "Chemsense ID" && parts[3] == "mac_address") return@forEach // these mac addresses have hex string values
//    val t =
//        (if (parts[0].contains('.')) dateParser
//        else dateParserNoMilli).parse(parts[0]).time
//    val c = parts[2] + ';' + parts[3]
//    val v = parts[4].toDoubleOrNull() ?: return@forEach // if cannot parse, skip
//  }
//
//  val data1 = CSVScan0(filepathA)


  @Test
  fun test() {
    val table1 = "exAPI"
    val data1: MutableList<NameTuple> = LinkedList()

    val tcvListFileAction = SensorFileAction.SensorFileActionImpl<MutableList<NameTuple>>(
        { data1 },
        { l,t,c,v ->
          l += mapOf("t" to t, "c" to c, "v" to v)
          l
        },
        { }
    )
    val urlA = Thread.currentThread().contextClassLoader.getResource(filepathA)
    tcvListFileAction.invoke(urlA)
    data1.sortWith(NameTupleOp.KeyComparator(initialSchema.keys))

    val A = ScanFromData(initialSchema, data1)
        .ext(filterFun)
        .ext(binFun)
        .ext(createCntFun)
        .sort(listOf("t'", "c", "t"))
        .agg(setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun))
        .ext(divideVnCntFun)

    val B = ScanFromData(initialSchema, data1)
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

    println()
    C.run().forEach { println(it) }

  }



}