package edu.washington.cs.laragraphulo.sensor

import edu.washington.cs.laragraphulo.api.PType
import edu.washington.cs.laragraphulo.parfile.FileAction
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import java.io.PrintStream
import java.net.URL
import java.text.SimpleDateFormat
import java.util.*

val dateParser = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").apply { timeZone = TimeZone.getTimeZone("UTC") }
val dateParserNoMilli = SimpleDateFormat("yyyy-MM-dd HH:mm:ss").apply { timeZone = TimeZone.getTimeZone("UTC") }
val dateParserNoTime = SimpleDateFormat("yyyy-MM-dd").apply { timeZone = TimeZone.getTimeZone("UTC") }

data class EncodeFunction(
    val encodeLong: (Long) -> ByteArray,
    val decodeLong: (ByteArray) -> Long,
    val encodeDouble: (Double) -> ByteArray,
    val decodeDouble: (ByteArray) -> Double
) {
  companion object {
    private val ullex = ULongLexicoder()
    private val dlex = DoubleLexicoder()
    private val nd = PType.DOUBLE.nullable
    val STRING = EncodeFunction(
        {it.toString().toByteArray()},
        {String(it).toLong()},
        {it.toString().toByteArray()},
        {String(it).toDouble()}
    )
    val LEX = EncodeFunction(
        ullex::encode,
        ullex::decode,
        dlex::encode,
        dlex::decode
    )
    val PTYPE = EncodeFunction(
        PType.LONG::encode,
        PType.LONG::decode,
        nd::encode,
        {nd.decode(it)!!}
    )
  }
}


/**
 * Ingest csv sensor data from parsed beehive files.
 */
interface SensorFileAction : FileAction {

  override fun run(f: URL) {
    invoke(f)
  }

  /**
   * Run the action on the sensor file
   */
  operator fun invoke(file: URL): Long


  companion object {

    fun printAction(out: PrintStream): SensorFileAction {
      return SensorFileActionImpl({}, { _, t, c, v -> out.println("t:$t\tc:$c\tv:$v") }, { _ -> Unit })
    }



    fun ingestAction(conn: Connector, table: String, encode: EncodeFunction,
                     recreateTable: Boolean = false,
                     partitions: Int = 1, splitFirstTime: Boolean = false,
                     reverse: Boolean = true, // files store times in reverse order
                     createTable: Boolean = true
    ): SensorFileAction {
      require(partitions >= 1) {"bad partitions: $partitions"}

      data class IngestState(
          val bw: BatchWriter,
          var m: Mutation,// = Mutation(tConv(0L)),
          var ms: Long = 0L
//          , var cnt: Long = 0
      )

      val tFirstFun: (URL) -> Long = { file ->
        file.openStream().reader().buffered().useLines { lines ->
          for (line in lines) {
            if (line.isBlank()) continue
            val parts = line.split(';')
            if (parts.size < 6) continue
            return@useLines (if (parts[0].contains('.')) dateParser
                else dateParserNoMilli).parse(parts[0]).time
          }
          throw RuntimeException("Cannot obtain time from first line in file $file")
        }
      }

      val beforeAction = { file: URL ->
        println("before: $file")
        val t = conn.tableOperations()
        if (recreateTable) {
          if (t.exists(table))
            t.delete(table)
          t.create(table)
        } else if (createTable) {
          if (!t.exists(table))
            t.create(table)
        }

        if (splitFirstTime || partitions > 1) {
          val tFirst = tFirstFun(file)
          val ss: SortedSet<Text> = TreeSet<Text>()
          if (splitFirstTime)
            ss.add(Text(encode.encodeLong(tFirst)))
          (1..partitions-1)
              .mapTo(ss) { Text(encode.encodeLong(tFirst + (if(reverse) -1 else 1)*it*1000L*60*60*24/partitions)) }
          println("Splits on $file: $ss")
          t.addSplits(table, ss)
          // p2 -> add day/2
          // p3 -> add day/3, 2*day/3
        }

        val bwc = BatchWriterConfig()
        val bw = conn.createBatchWriter(table, bwc)
        IngestState(bw, Mutation())
      }

      val EMPTY = byteArrayOf()
      val tcvAction = { s: IngestState, t: Long, c: String, v: Double ->
//        println("see: $t, $c, $v;  $s")
        if (t != s.ms) {
          if (s.m.size() > 0) s.bw.addMutation(s.m)
          s.m = Mutation(encode.encodeLong(t))
          s.ms = t
        }
        s.m.put(EMPTY, c.toByteArray(), encode.encodeDouble(v))
        s
      }

      val afterAction = { s: IngestState ->
//        println("after: $s")
        if (s.m.size() > 0) s.bw.addMutation(s.m)
        s.bw.close()
      }
      return SensorFileActionImpl(beforeAction, tcvAction, afterAction)
    }

  }


  class SensorFileActionImpl<State>(
      private val beforeAction: (URL) -> State,
      private val tcvAction: (s: State, t: Long, c: String, v: Double) -> State,
      private val afterAction: (s: State) -> Unit
  ) : SensorFileAction {

    override operator fun invoke(file: URL): Long {
      var s = beforeAction(file)
      var cnt = 0L
      try {
        file.openStream().reader().buffered().useLines { lines ->
          lines.forEach { line ->
            //        2017-02-06 11:27:44.976000;coresense:3;TSYS01;temperature;8.22;NO_UNIT
//        DATE;_;(CLASS;FAMILY);VALUE;_
//        t           c          v
            val parts = line.split(';')
            if (parts.size < 6) return@forEach
            if (parts[2] == "Chemsense ID" && parts[3] == "mac_address") return@forEach // these mac addresses have hex string values
            val t =
                (if (parts[0].contains('.')) dateParser
                else dateParserNoMilli).parse(parts[0]).time
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