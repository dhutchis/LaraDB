package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.sensor.SensorFileAction
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

    val X = listOf(
        NameTupleOp.ScanFromData(initialSchema, data1),
        NameTupleOp.ScanFromData(initialSchema, data1)
    )
        .map { NameTupleOp.Ext(it, filterFun) }
        .map { NameTupleOp.Ext(it, binFun) }
        .map { NameTupleOp.Ext(it, createCntFun) }
//    .apply { println("after ext and create cnt: ${this.first().resultSchema}") }
        .map { NameTupleOp.Sort(it, listOf("t'", "c", "t")) }
        .map { NameTupleOp.MergeUnion0.MergeAgg(it, setOf("t'", "c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) } // fails here; need a re-sort operation
        .map { NameTupleOp.Ext(it, divideVnCntFun) }
//        .apply { this.first().run().forEach { println(it) } }
        .run { NameTupleOp.MergeJoin(this[0], this[1], mapOf("v" to subtractVn)) }

    val N = NameTupleOp.Ext(X, notNullFun)
        .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf("t'"), mapOf("v" to anyFun)) }
//        .apply { this.run().forEach { println(it) } }
        .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf(), mapOf("v" to plusIntFun)) }

    val X0 = NameTupleOp.Sort(X, listOf("c", "t'"))

    val M = NameTupleOp.Ext(X0, createCntFun)
        .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf("c"), mapOf("v" to plusDoubleNullFun, "cnt" to plusIntFun)) }
        .run { NameTupleOp.Ext(this, divideVnCntFun) }

    val U = NameTupleOp.MergeJoin(X0, M, mapOf("v" to subtractVn))
        .run { NameTupleOp.Sort(this, listOf("t'","c")) }

    val C = NameTupleOp.MergeJoin(U, NameTupleOp.Rename(U, mapOf("c" to "c'")), mapOf("v" to multiplyVn))
//        .apply { println(this.resultSchema) }
//        .apply { this.run().forEach { println(it) } }
        .run { NameTupleOp.Sort(this, listOf("c", "c'", "t'")) }
        .run { NameTupleOp.MergeUnion0.MergeAgg(this, setOf("c", "c'"), mapOf("v" to plusDoubleNullFun)) }
        .run { NameTupleOp.MergeJoin(this, N, mapOf("v" to divideMinusOneFun)) }

    println()
    C.run().forEach { println(it) }

  }



}