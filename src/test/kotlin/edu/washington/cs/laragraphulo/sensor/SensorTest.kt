package edu.washington.cs.laragraphulo.sensor

import edu.washington.cs.laragraphulo.AccumuloTestBase
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.junit.Ignore
import org.junit.Test
import java.io.File
import java.util.*

const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"
const val filepathB = "data/sensor/bee-denver-v2dec-2017-02-06-tiny.txt"

//val minTime = dateParserNoTime.parse("2017-02-06").time // start at 6th - 1486339200000
//val maxTime = dateParserNoTime.parse("2017-02-12").time // end at 11th  -

private inline fun timePrint(s: String, f: () -> Any?) {
  val (_, t) = time(f)
  println("TIME $s: $t")
}

class SensorTest : AccumuloTestBase() {
  private val conn = tester.accumuloConfig.connector

  private val opts: Set<SensorCalc.SensorOpt> = {
    val s = EnumSet.noneOf(SensorCalc.SensorOpt::class.java)
    s.add(SensorCalc.SensorOpt.Encode)
    s.add(SensorCalc.SensorOpt.FilterPush)
    s.add(SensorCalc.SensorOpt.MonotoneSortElim)
    s.add(SensorCalc.SensorOpt.ZeroDiscard)
    s.add(SensorCalc.SensorOpt.AggregatePush)
    s.add(SensorCalc.SensorOpt.PropagatePartition)
    s.add(SensorCalc.SensorOpt.SymmetricCovariance)
    s.add(SensorCalc.SensorOpt.Defer)
    s.add(SensorCalc.SensorOpt.ReuseSource)
    s
  }()

  /**
   * This parallel ingest class needs to be revisited.
   * It apparently works with two files inserted into the same table.
   */
  @Test
  fun testInsertToOne() {
    val tn = "testInsertToOne"
    val fs = setOf(filepathA, filepathB)
        .map { Thread.currentThread().contextClassLoader.getResource(it).path.let(::File) }
        .toSet()
    insertToOne(fs, tn)
  }


  /**
   * This is the main experiment method for running the Sensor query
   * on the full data set in `data/sensor/input/`.
   */
  @Test
  @Ignore
  fun doReal() {
    val dir = Thread.currentThread().contextClassLoader.getResource("data/sensor/input/").path.run(::File)
    println("Input Dir: $dir")
    val fs = dir.listFiles({ _, name -> name.startsWith("bee-") && name.endsWith(".txt") })
    val (fsA, fsB) = fs.partition { it.name.contains("-uw-") }
    println("FilesA   : ${fsA.joinToString {it.name}}")
    println("FilesB   : ${fsB.joinToString {it.name}}")
    val tA = "bee_uw"
    val tB = "bee_denver"
    val mint = dateParserNoTime.parse("2017-01-10").time 
    val maxt = dateParserNoTime.parse("2017-02-10").time 
    val cp = 10
    SensorCalcDriver(fsA.toSet(), fsB.toSet(), tA, tB, mint, maxt,
        cp, conn, tester.accumuloConfig.authenticationToken as PasswordToken)
        .doMany()
  }


  fun insertToOne(fs: Set<File>, tn: String) {
    timePrint("insertToOne") {
      SensorFileListInsert(fs,
          conn, tn, SensorCalc.SensorOpt.Encode in opts).run()
    }
  }

}
