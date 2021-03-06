package edu.washington.cs.laragraphulo.util

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.Authorizations
import java.io.PrintWriter
import java.util.SortedMap
import java.util.TreeMap
import java.util.TreeSet


/**
 * Helpful for debugging
 */
object DebugUtil {

  /** @param w Label Width of rows and columns; default 5
   */
  @JvmOverloads fun printTable(header: String?, conn: Connector, table: String, w: Int = 5,
                               write: (String) -> Unit = ::print,
                               valFun: (Value) -> String = Value::toString) {
    if (header != null)
      write(header+'\n')
    val scan: Scanner
    try {
      scan = conn.createScanner(table, Authorizations.EMPTY)
    } catch (e: TableNotFoundException) {
      throw RuntimeException(e)
    }

    printMapFull(scan.iterator(), w, write, valFun)
    scan.close()
  }


  /** @param w Label Width of rows and columns; default 5
   */
  fun printMapFull(iter: Iterator<Map.Entry<Key, Value>>, w: Int = 5,
                   write: (String) -> Unit = ::print,
                   valFun: (Value) -> String = Value::toString) {
    val columnSet = TreeSet<String>()
    val rowToColumnMap = TreeMap<String, SortedMap<String, String>>()

    run {
      var curRow: String? = null
      var curRowMap: SortedMap<String, String>? = null

      while (iter.hasNext()) {
        val entry = iter.next()
        val k = entry.key
        val row = k.row.toString()
        val col = k.columnQualifier.toString()

        columnSet.add(col)
        if (row != curRow) {
          curRow = row
          curRowMap = TreeMap<String, String>()
          rowToColumnMap.put(curRow, curRowMap)
        }
        curRowMap!!.put(col, valFun(entry.value))
      }
    }

    // print columns
    write(String.format("%${w}s ", ""))
    for (col in columnSet) {
      write(String.format("%${w}s ", col.substring(0, Math.min(w, col.length))))
    }
    write("\n")

    // print body
    for ((row, colMap) in rowToColumnMap) {
      write(String.format("%${w}s ", row.substring(0, Math.min(w, row.length))))

      for (col in columnSet) {
        if (col in colMap) {
          val v = colMap[col]!!
          write(String.format("%${w}s ", v.substring(0, Math.min(w, v.length))))
        } else {
          write(String.format("%${w}s ", ""))
        }
      }
      write("\n")
    }
  }

}
