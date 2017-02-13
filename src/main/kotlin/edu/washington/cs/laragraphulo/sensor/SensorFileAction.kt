package edu.washington.cs.laragraphulo.sensor

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import java.io.File
import java.io.PrintStream
import java.text.SimpleDateFormat
import java.util.*

private val dateParser = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").apply { timeZone = TimeZone.getTimeZone("UTC") }


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
    private data class IngestState(
        val bw: BatchWriter,
        var m: Mutation,// = Mutation(tConv(0L)),
        var ms: Long = 0L
//          , var cnt: Long = 0
    )

    fun ingestAction(conn: Connector, table: String, encode: Boolean,
                     recreateTable: Boolean = false,
                     partitions: Int = 1, splitFirstTime: Boolean = false,
                     reverse: Boolean = true // files store times in reverse order
    ): SensorFileAction {
      require(partitions >= 1) {"bad partitions: $partitions"}

      val tFirstFun: (File) -> Long = { file: File ->
        file.useLines { lines ->
          for (line in lines) {
            if (line.isBlank()) continue
            val parts = line.split(';')
            if (parts.size < 6) continue
            return@useLines dateParser.parse(parts[0]).time
          }
          throw RuntimeException("Cannot obtain time from first line in file $file")
        }
      }

      val beforeAction = { file: File ->
        if (recreateTable) {
          if (conn.tableOperations().exists(table))
            conn.tableOperations().delete(table)
          conn.tableOperations().create(table)
        } else {
          if (!conn.tableOperations().exists(table))
            conn.tableOperations().create(table)
        }

        if (splitFirstTime || partitions > 1) {
          val tFirst = tFirstFun(file)
          val ss: SortedSet<Text> = TreeSet<Text>()
          if (splitFirstTime)
            ss.add(Text(tFirst.toByteArray(encode)))
          (1..partitions-1)
              .mapTo(ss) { Text((tFirst + (if(reverse) -1 else 1)*it*1000L*60*60*24/partitions)
                  .toByteArray(encode)) }
          println("Splits on $file: $ss")
          conn.tableOperations().addSplits(table, ss)
          // p2 -> add day/2
          // p3 -> add day/3, 2*day/3
        }

        val bwc = BatchWriterConfig()
        val bw = conn.createBatchWriter(table, bwc)
        IngestState(bw, Mutation((0L).toByteArray(encode)))
      }

      val EMPTY = byteArrayOf()
      val tcvAction = { s: IngestState, t: Long, c: String, v: Double ->
        if (t != s.ms) {
          if (s.m.size() > 0) s.bw.addMutation(s.m)
          s.m = Mutation(t.toByteArray(encode))
          s.ms = t
        }
        s.m.put(EMPTY, c.toByteArray(), v.toByteArray(encode))
        s
      }

      val afterAction = { s: IngestState ->
        s.bw.addMutation(s.m)
        s.bw.close()
      }
      return SensorFileActionImpl(beforeAction, tcvAction, afterAction)
    }

  }


  class SensorFileActionImpl<State>(
      private val beforeAction: (File) -> State,
      private val tcvAction: (s: State, t: Long, c: String, v: Double) -> State,
      private val afterAction: (s: State) -> Unit
  ) : SensorFileAction {

    override operator fun invoke(file: File): Long {
      var s = beforeAction(file)
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

  }
}