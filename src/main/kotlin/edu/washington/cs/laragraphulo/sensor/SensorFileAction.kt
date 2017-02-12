package edu.washington.cs.laragraphulo.sensor

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder
import org.apache.accumulo.core.data.Mutation
import java.io.File
import java.io.PrintStream
import java.text.SimpleDateFormat
import java.util.*

typealias tcvAction = (t: Long, c: String, v: Double) -> Unit




/**
 * Ingest csv sensor data from parsed beehive files.
 */
interface SensorFileAction {
  /**
   * Run the action on the sensor file
   */
  operator fun invoke(file: File): Long


  companion object {

    fun printAction(out: PrintStream): SensorFileAction {
      return SensorFileActionImpl({}, { _, t, c, v -> out.println("t:$t\tc:$c\tv:$v") }, { _ -> Unit })
    }

    /** This should be defined inside the ingestAction method but some Kotlin Compiler error in 1.1-Beta2 prevents it. */
    private data class State(
        val bw: BatchWriter,
        var m: Mutation,// = Mutation(tConv(0L)),
        var ms: Long = 0L
//          , var cnt: Long = 0
    )

    fun ingestAction(conn: Connector, table: String, encode: Boolean): SensorFileAction {
      val ull = ULongLexicoder()
      val EMPTY = byteArrayOf()
      val dl = DoubleLexicoder()

//      val tConv: (Long) -> ByteArray =
//          if (encode) { t: Long -> t.toString().toByteArray() }
//          else { t: Long -> ullex.encode(t) }
      val vConv: (Double) -> ByteArray =
          if (encode) { v: Double -> dl.encode(v) }
          else { v: Double -> v.toString().toByteArray() }



      val beforeAction = {
        if (conn.tableOperations().exists(table))
          conn.tableOperations().delete(table)
        conn.tableOperations().create(table)
        val bwc = BatchWriterConfig()
        val bw = conn.createBatchWriter(table, bwc)
        State(bw, Mutation((0L).toByteArray(encode)))
      }

      val tcvAction = { s: State, t: Long, c: String, v: Double ->
        if (t != s.ms) {
          if (s.m.size() > 0) s.bw.addMutation(s.m)
          s.m = Mutation(t.toByteArray(encode))
          s.ms = t
        }
        s.m.put(EMPTY, c.toByteArray(), vConv(v))
//        s.cnt++
        s
      }

      val afterAction = { s: State ->
        s.bw.addMutation(s.m)
        s.bw.close()
      }

      return SensorFileActionImpl(beforeAction, tcvAction, afterAction)
    }

  }


  class SensorFileActionImpl<State>(
      private val beforeAction: () -> State,
      private val tcvAction: (s: State, t: Long, c: String, v: Double) -> State,
      private val afterAction: (s: State) -> Unit
  ) : SensorFileAction {

    override operator fun invoke(file: File): Long {
      var s = beforeAction()
      var cnt = 0L
      try {
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

            s = tcvAction(s, t, c, v)
            cnt++
          }
        }
      } finally {
        afterAction(s)
      }
      return cnt
    }


    companion object {
      private val dateParser = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").apply { timeZone = TimeZone.getTimeZone("UTC") }
    }

  }
}