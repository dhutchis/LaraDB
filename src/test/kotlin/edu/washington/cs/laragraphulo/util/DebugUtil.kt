package edu.washington.cs.laragraphulo.util

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.Authorizations
import java.util.SortedMap
import java.util.TreeMap
import java.util.TreeSet


/**
 * Helpful for debugging
 */
object DebugUtil {

  /** @param w Label Width of rows and columns; default 5
   */
  @JvmOverloads fun printTable(header: String?, conn: Connector, table: String, w: Int = 5) {
    if (header != null)
      println(header)
    val scan: Scanner
    try {
      scan = conn.createScanner(table, Authorizations.EMPTY)
    } catch (e: TableNotFoundException) {
      throw RuntimeException(e)
    }

    printMapFull(scan.iterator(), w)
    scan.close()
  }


  /** @param w Label Width of rows and columns; default 5
   */
  fun printMapFull(iter: Iterator<Map.Entry<Key, Value>>, w: Int = 5) {
    val columnSet = TreeSet<String>()
    val rowToColumnMap = TreeMap<String, SortedMap<String, Value>>()

    run {
      var curRow: String? = null
      var curRowMap: SortedMap<String, Value>? = null

      while (iter.hasNext()) {
        val entry = iter.next()
        val k = entry.key
        val row = k.row.toString()
        val col = k.columnQualifier.toString()

        columnSet.add(col)
        if (row != curRow) {
          curRow = row
          curRowMap = TreeMap<String, Value>()
          rowToColumnMap.put(curRow, curRowMap)
        }
        curRowMap!!.put(col, entry.value)
      }
    }

    // print columns
    System.out.printf("%${w}s ", "")
    for (col in columnSet) {
      System.out.printf("%${w}s ", col.substring(0, Math.min(w, col.length)))
    }
    println()

    // print body
    for ((row, colMap) in rowToColumnMap) {
      System.out.printf("%${w}s ", row.substring(0, Math.min(w, row.length)))

      for (col in columnSet) {
        if (colMap.containsKey(col)) {
          val v = colMap[col].toString()
          System.out.printf("%${w}s ", v.substring(0, Math.min(w, v.length)))
        } else {
          System.out.printf("%${w}s ", "")
        }
      }
      println()
    }
  }

}
