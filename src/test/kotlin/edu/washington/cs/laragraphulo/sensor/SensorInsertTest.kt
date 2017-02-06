package edu.washington.cs.laragraphulo.sensor

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.Loggable
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger
import java.io.BufferedInputStream

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.net.URL
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*


const val filepath = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt";
const val tablename = "bee_uw_20170206";

typealias tcvAction = (t:Long, c:String, v:Double) -> Unit

/**
 * Insert sensor data from a file in test/resources/
 * into Accumulo.
 */
class SensorInsertTest : AccumuloTestBase() {

  init {
    logger.debug("Initializing SensorInsertTest")
  }

  @Test
  @Throws(FileNotFoundException::class)
  fun put() {
    val conn = tester.accumuloConfig.connector

    val url: URL = Thread.currentThread().contextClassLoader.getResource(filepath)
    Assert.assertNotNull(url)
    val file = File(url.path)

    val tcvLog: tcvAction = { t,c,v ->
      println("t:$t\tc:$c\tv:$v")
    }

    // deleteExistingTables
    val cnt = putSensorFile(file, tcvLog)
    logger.info("Wrote $cnt entries to $tablename")
  }



  companion object : Loggable {
    override val logger: Logger = logger<SensorInsertTest>()

    val dateParser = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").apply { timeZone = TimeZone.getTimeZone("UTC") }


    private inline fun putSensorFile(file: File, tcvAction: tcvAction) {
      file.bufferedReader().useLines { lines -> lines.forEach { line ->
//        2017-02-06 11:27:44.976000;coresense:3;TSYS01;temperature;8.22;NO_UNIT
//        DATE;_;(CLASS;FAMILY);VALUE;_
//        t           c          v
        val parts = line.split(';')
        if (parts.size < 6) return
        if (parts[2] == "Chemsense ID" && parts[3]=="mac_address") return // these mac addresses have hex string values
        val t = dateParser.parse(parts[0]).time
        val c = parts[2]+';'+parts[3]
        val v = parts[4].toDoubleOrNull() ?: return // if cannot parse, skip
        tcvAction(t,c,v)

      } }
    }


  }


}
