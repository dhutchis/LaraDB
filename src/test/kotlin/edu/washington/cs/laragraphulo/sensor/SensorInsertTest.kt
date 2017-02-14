package edu.washington.cs.laragraphulo.sensor

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.util.DebugUtil
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.io.Text
import org.junit.*
import org.junit.jupiter.api.Disabled
import org.junit.runners.MethodSorters
import java.io.File
import java.io.FilenameFilter
import java.net.URL
import java.util.*
import kotlin.system.measureTimeMillis

// PARAMETERS:
const val DOINGEST = true // ingest input tables
const val DOINGEST_RECREATE = true // recreate input tables
const val DODB = true

// change to set of files
const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"
const val filepathB = "data/sensor/bee-denver-v2dec-2017-02-06-tiny.txt"
const val tablenameA = "bee_uw_20170206"
const val tablenameB = "bee_denver_20170206"
const val minTime = 0L
const val maxTime = Long.MAX_VALUE
const val cPartitions = 2
const val tPartitions = 2  // change to split after every file except the last

//const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-11.txt"
//const val filepathB = "data/sensor/bee-denver-v2dec-2017-02-11.txt"
//const val tablenameA = "bee_uw_20170211"
//const val tablenameB = "bee_denver_20170211"
//val minTime = dateParserNoTime.parse("2017-02-06").time // start at 6th - 1486339200000
//val maxTime = dateParserNoTime.parse("2017-02-12").time // end at 11th  -

private inline fun timePrint(s: String, f: () -> Any?) {
  val (_, t) = time(f)
  println("TIME $s: $t")
}

/**
 * Insert sensor data from a file in test/resources/
 * into Accumulo.
 */
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class SensorInsertTest : AccumuloTestBase() {
  private val conn = tester.accumuloConfig.connector

  private val opts: Set<SensorCalc.SensorOpt> = {
    val s = EnumSet.noneOf(SensorCalc.SensorOpt::class.java)
//    s.add(SensorCalc.SensorOpt.Encode)
//    s.add(SensorCalc.SensorOpt.FilterPush)
//    s.add(SensorCalc.SensorOpt.MonotoneSortElim)
//    s.add(SensorCalc.SensorOpt.ZeroDiscard)
//    s.add(SensorCalc.SensorOpt.AggregatePush)
//    s.add(SensorCalc.SensorOpt.PropagatePartition)
//    s.add(SensorCalc.SensorOpt.SymmetricCovariance)
//    s.add(SensorCalc.SensorOpt.Defer)
//    s.add(SensorCalc.SensorOpt.ReuseSource)
    s
  }()

  private val scc = SensorCalc(conn, tester.accumuloConfig.authenticationToken as PasswordToken,
      tablenameA, tablenameB,
      opts, cPartitions
  )
  private val SHOW: List<String> = listOf() //scc.sensorC, scc.sensorM if Defer


  @Test
  fun testInsertToOne() {
    val tn = "testInsertBoth"
    val fs = setOf(filepathA, filepathB)
        .map { Thread.currentThread().contextClassLoader.getResource(it).path.let { File(it) } }
        .toSet()
    insertToOne(fs, tn)
  }

  @Test
  @Disabled @Ignore
  fun doReal() {
    val dir = Thread.currentThread().contextClassLoader.getResource("data/sensor/input/").path.run(::File)
    println("Input Dir: $dir")
    val fs = dir.listFiles({ _, name -> name.startsWith("bee-") && name.endsWith(".txt") })
    val (fsA, fsB) = fs.partition { it.name.contains("-uw-") }
    println("FilesA   : ${fsA.joinToString {it.name}}")
    println("FilesB   : ${fsB.joinToString {it.name}}")
    val tA = "bee_uw"
    val tB = "bee_denver"
    val mint = dateParserNoTime.parse("2017-02-06").time // start at 6th - 1486339200000
    val maxt = dateParserNoTime.parse("2017-02-12").time // end at 11th  -
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
//    DebugUtil.printTable(tn, conn, tn, 14)
//        { it.get().toDouble(SensorCalc.SensorOpt.Encode in opts).toString() }
  }


  @Test
  fun testAll() {
//    val t = conn.tableOperations()
//    val tn = "test"
//    t.create(tn)
//    var tm: SortedSet<Text> = TreeSet()
//    tm.add(Text("b"))
//    t.addSplits(tn, tm)
//    tm = TreeSet()
//    tm.add(Text("a"))
//    t.addSplits(tn, tm)
//    println(t.listSplits(tn))
//    return

    // insert file set A to table A
    timePrint("aInsert") { aInsert() }
    // insert file set B to table B
    timePrint("bInsert") { bInsert() }
    if (!DODB) return

    println("Running: ${opts.printSet()}")
    val times = scc.timeAll(minTime, maxTime)

    SHOW.forEach {
      DebugUtil.printTable(it, conn, it, 14)
          { it.get().toDouble(SensorCalc.SensorOpt.Encode in opts).toString() }
    }

    println(times)

//    var tCount: Long = -1
//    time("cBinAndDiff") { tCount = cBinAndDiff() }
//    time("dMeanAndSubtract") { dMeanAndSubtract() }
//    time("eCovariance") { eCovariance(tCount) }
    if (SensorCalc.SensorOpt.Encode !in opts)
      conn.tableOperations().setProperty(scc.sensorC, "table.formatter", "edu.washington.cs.laragraphulo.sensor.DoubleValueDisplay")
  }


  fun aInsert() {
    println("Inserting $tablenameA")
    insert(filepathA, tablenameA)
  }

  fun bInsert() {
    println("Inserting $tablenameB")
    insert(filepathB, tablenameB)
  }

  private fun insert(filepath: String, tablename: String) {
    Assume.assumeTrue("not ingesting", DOINGEST)
    val url: URL = Thread.currentThread().contextClassLoader.getResource(filepath)
    Assert.assertNotNull(url)
    val file = File(url.path)
    val action =
        if (DODB) SensorFileAction.ingestAction(conn, tablename, SensorCalc.SensorOpt.Encode in opts,
            recreateTable = DOINGEST_RECREATE, partitions = tPartitions, splitFirstTime = false)
        else SensorFileAction.printAction(System.out)
    val cnt = action(file)
    println("Wrote $cnt entries to $tablename from $filepath")
//    DebugUtil.printTable(tablename, conn, tablename, 14)
  }


  fun cBinAndDiff(): Long {
    Assume.assumeTrue(DODB)
    val tCount = scc.binAndDiff(minTime, maxTime)

//      DebugUtil.printTable(scc.sensorX, conn, scc.sensorX, 14)
    return tCount
  }

  fun dMeanAndSubtract() {
    Assume.assumeTrue(DODB)
    scc.meanAndSubtract()

//      DebugUtil.printTable(scc.sensorU, conn, scc.sensorU, 14)
  }

  fun eCovariance(tCount: Long) {
    Assume.assumeTrue(DODB)
    Assume.assumeTrue("Positive tCount $tCount", tCount > 0L)
    scc.covariance(tCount)
//    DebugUtil.printTable(scc.sensorC, conn, scc.sensorC, 14)
  }



}
