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
import java.text.SimpleDateFormat
import java.util.*
import kotlin.system.measureTimeMillis

// PARAMETERS:
const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-small.txt"
const val filepathB = "data/sensor/bee-denver-v2dec-2017-02-06-small.txt"
const val tablenameA = "bee_uw_20170206"
const val tablenameB = "bee_denver_20170206"
const val DODB = true
const val DOSTRING = false

private val ull = ULongLexicoder()
private val tConv: (Long) -> ByteArray =
    if (DOSTRING) { t: Long -> t.toString().toByteArray() }
    else { t: Long -> ull.encode(t) }
private val dl = DoubleLexicoder()
private val vConv: (Double) -> ByteArray =
    if (DOSTRING) { v: Double -> v.toString().toByteArray() }
    else { v: Double -> dl.encode(v) }
private val EMPTY = byteArrayOf()

typealias tcvAction = (t: Long, c: String, v: Double) -> Unit

private inline fun time(s: String, f: () -> Unit) {
  println("TIME $s ${measureTimeMillis(f)/1000.0}")
}

/**
 * Insert sensor data from a file in test/resources/
 * into Accumulo.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class SensorInsertTest : AccumuloTestBase() {

  private val origReuse = tester.requestReuse
  private val conn = tester.accumuloConfig.connector
  private val scc = SensorCovarianceCalc(conn, tester.accumuloConfig.authenticationToken as PasswordToken,
      tablenameA, tablenameB, doString = DOSTRING)



  @Test
  fun testAll() {
    time("aInsert") { aInsert() }
    time("bInsert") { bInsert() }
    var tCount: Long = -1
    time("cBinAndDiff") { tCount = cBinAndDiff() }
    time("dMeanAndSubtract") { dMeanAndSubtract() }
    time("eCovariance") { eCovariance(tCount) }
    if (!DOSTRING)
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
    var cnt = 0L

    if (DODB) {
      if (conn.tableOperations().exists(tablename))
        conn.tableOperations().delete(tablename)
      conn.tableOperations().create(tablename)

      val bwc = BatchWriterConfig()
      conn.createBatchWriter(tablename, bwc).use { bw ->

        var m = Mutation(tConv(0L))
        var ms = 0L

        val tcvInsertDB: tcvAction = { t, c, v ->
          if (t != ms) {
            if (m.size() > 0) bw.addMutation(m)
            m = Mutation(tConv(t))
            ms = t
          }
          m.put(EMPTY, c.toByteArray(), vConv(v))
          cnt++
        }

        putSensorFile(file, tcvInsertDB)
        bw.addMutation(m)
        bw.flush()

      }
    } else {
      /** Prints parsed file contents */
      val tcvLog: tcvAction = { t, c, v ->
        println("t:$t\tc:$c\tv:$v")
        cnt++
      }
      putSensorFile(file, tcvLog)
    }

//    logger.info
    println("Wrote $cnt entries to $tablename")
//    DebugUtil.printTable(tablename, conn, tablename, 14)
  }


  fun cBinAndDiff(): Long {
    Assume.assumeTrue(DODB)
    val tCount = scc.binAndDiff()

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
    scc.covariance(tCount)
//    DebugUtil.printTable(scc.sensorC, conn, scc.sensorC, 14)
  }


  companion object //: Loggable {
  {
//    override val logger: Logger = logger<SensorInsertTest>()

    val dateParser = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").apply { timeZone = TimeZone.getTimeZone("UTC") }


    private inline fun putSensorFile(file: File, tcvAction: tcvAction) {
      file.bufferedReader().useLines { lines ->
        lines.forEach { line ->
          //        2017-02-06 11:27:44.976000;coresense:3;TSYS01;temperature;8.22;NO_UNIT
//        DATE;_;(CLASS;FAMILY);VALUE;_
//        t           c          v
          val parts = line.split(';')
          if (parts.size < 6) return@forEach
          if (parts[2] == "Chemsense ID" && parts[3] == "mac_address") return@forEach // these mac addresses have hex string values
          val t = dateParser.parse(parts[0]).time
          val c = parts[2] + ';' + parts[3]
          val v = parts[4].toDoubleOrNull() ?: return@forEach // if cannot parse, skip
//        println("Inserting $t, $c, $v")
          tcvAction(t, c, v)

        }
      }
    }


  }


}
