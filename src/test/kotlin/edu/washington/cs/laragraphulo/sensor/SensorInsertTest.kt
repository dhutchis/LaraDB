package edu.washington.cs.laragraphulo.sensor

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.util.DebugUtil
import kotlinx.support.jdk7.use
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Mutation
import org.junit.Assert
import org.junit.Assume
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runners.MethodSorters
import java.io.File
import java.net.URL
import java.security.spec.EncodedKeySpec
import java.text.SimpleDateFormat
import java.util.*
import kotlin.system.measureTimeMillis

// PARAMETERS:
const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-small.txt"
const val filepathB = "data/sensor/bee-denver-v2dec-2017-02-06-small.txt"
const val tablenameA = "bee_uw_20170206"
const val tablenameB = "bee_denver_20170206"
const val DODB = true
const val minTime = 0L
const val maxTime = Long.MAX_VALUE

private inline fun time(s: String, f: () -> Unit) {
  println("TIME $s ${measureTimeMillis(f)/1000.0}")
}

/**
 * Insert sensor data from a file in test/resources/
 * into Accumulo.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class SensorInsertTest : AccumuloTestBase() {
  private val conn = tester.accumuloConfig.connector

  private val opts: Set<SensorCalc.SensorOpt> = {
    val s = EnumSet.noneOf(SensorCalc.SensorOpt::class.java)
    s.add(SensorCalc.SensorOpt.Encode)
    s.add(SensorCalc.SensorOpt.FilterPush)
    s.add(SensorCalc.SensorOpt.MonotoneSortElim)
    s
  }()

  private val scc = SensorCalc(conn, tester.accumuloConfig.authenticationToken as PasswordToken,
      tablenameA, tablenameB,
      opts
  )



  @Test
  fun testAll() {
    time("aInsert") { aInsert() }
    time("bInsert") { bInsert() }
    var tCount: Long = -1
    time("cBinAndDiff") { tCount = cBinAndDiff() }
    time("dMeanAndSubtract") { dMeanAndSubtract() }
    time("eCovariance") { eCovariance(tCount) }
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
    val url: URL = Thread.currentThread().contextClassLoader.getResource(filepath)
    Assert.assertNotNull(url)
    val file = File(url.path)
    val action =
        if (DODB) SensorFileAction.ingestAction(conn, tablename, SensorCalc.SensorOpt.Encode in opts)
        else SensorFileAction.printAction(System.out)
    val cnt = action(file)
//    logger.info
    println("Wrote $cnt entries to $tablename")
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
