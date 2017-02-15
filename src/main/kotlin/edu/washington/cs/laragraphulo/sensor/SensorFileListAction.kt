package edu.washington.cs.laragraphulo.sensor

import edu.washington.cs.laragraphulo.parfile.FileAction
import edu.washington.cs.laragraphulo.parfile.ParallelFileMapper
import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.io.Text
import java.io.File
import java.util.*

/**
 * Do a [SensorFileAction] on many files in parallel.
 */
class SensorFileListInsert(
    fs: Set<File>,
    private val conn: Connector,
    private val table: String,
    private val encode: Boolean,
    val lockDir: File = File("lock/"),
    /** Number of Threads */
    val NT: Int = 2
) : Runnable {
  private val files: List<SensorFile> =
      fs.mapTo(ArrayList(), ::SensorFile)
          .sortedBy { it:SensorFile -> it.date  }

  override fun run() {
    val t = conn.tableOperations()
    if (!t.exists(table)) {
      // only do this if we are the original table creator
      t.create(table)
      // Splits - set at odd date boundaries, after removing first // clean 350k entries x 5 tablets per table
      val dates = files.map { it.date }.sorted()
          .let { it.subList(1,it.size) }
          .filterIndexed { index, l -> index % 3 == 1 }
          .map { Text(it.toByteArray(encode)) }.toSortedSet()
      t.addSplits(table, dates)
//      println(files)
//      println(files.map { it.type to it.date })
      println("Table $table splits: $dates")
      println(files.map { it.file.nameWithoutExtension to it.date })
    }

    val fa: FileAction = SensorFileAction.ingestAction(conn, table, encode,
        createTable = false)
//        object : FileAction {
//      override fun run(f: File) {
//        SensorFileAction.ingestAction(conn, table, encode,
//            createTable = false).run(f)
//      }
//    }

    if (!lockDir.exists())
      lockDir.mkdirs()

    ParallelFileMapper(files.map { it.file }, lockDir, fa).run()

//    val threads = Array(NT) { Thread(ParallelFileMapper(files.map { it.file }, lockDir, fa), "t$it") }
//    threads.forEach(Thread::start)
//    threads.forEach(Thread::join)
  }

  private data class SensorFile(
      val file: File
  ) {
    /** "uw" or "denver" */
    val type: String
    /** Date given in milliseconds. */
    val date: Long

    init {
      // bee-denver-v2dec-2017-02-02.txt
      val (t, d) = Scanner(file.name).use {
        try {
          it.apply { findInLine("bee-(\\w+)-v2dec-(\\d{4}-\\d{2}-\\d{2}).*\\.txt") }
              .match().let {
            it.group(1) to dateParserNoTime.parse(it.group(2)).time
          }
        } catch (e: IllegalStateException) {
          throw RuntimeException("Problem parsing: ${file.name}", e)
        }
      }
      type = t
      date = d
    }
  }


}