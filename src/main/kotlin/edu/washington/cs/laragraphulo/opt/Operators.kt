package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.lang3.ArrayUtils
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import java.util.*

class CSVScan(val url: Op<URL>,
              val accessPath: Op<AccessPath>,
              /**
               * For each csv column, define an attribute name and a conversion from String to byte[]
               */
              val csvSchema: Op<List<Pair<String, LexicoderPlus<String>>>>,
              val delimiter: Op<Char> = Obj(',')
) : Op<IteratorFlow>(url, accessPath) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}

private class CSVScan_impl(val url: URL,
                           val accessPath: AccessPath,
                           /**
                            * For each csv column, define an attribute name and a conversion from String to byte[]
                            */
                           val csvSchema: List<Pair<String, LexicoderPlus<String>>>,
                           val delimiter: Char = ','
)

private fun doit(csvScan: CSVScan_impl): Iterator<Pair<Key, Value>> {
//  val encodeds = Array<ByteArray?>(csvScan.accessPath.allAttributes.size, {null})
  val parser = CSVParser(BufferedReader(InputStreamReader(csvScan.url.openStream())),
      CSVFormat.newFormat(csvScan.delimiter))
  val dap: List<Int> = csvScan.accessPath.dap.map { attr -> csvScan.csvSchema.indexOfFirst { attr.name == it.first } }
  val lap: List<Int> = csvScan.accessPath.lap.map { attr -> csvScan.csvSchema.indexOfFirst { attr.name == it.first } }
  val cap: List<Pair<String, List<Pair<String,Int>>>> = csvScan.accessPath.cap.map { cfpair -> cfpair.key to cfpair.value.attributes.map { attrpair -> attrpair.key to csvScan.csvSchema.indexOfFirst { attrpair.key == it.first } }}

  return object: Iterator<Pair<Key, Value>> {

    val csviter = parser.iterator()
    var toiter: Iterator<Pair<Key, Value>> = Collections.emptyIterator()

    override fun hasNext(): Boolean {
      return toiter.hasNext() || csviter.hasNext()
    }

    override fun next(): Pair<Key, Value> {
      if (!toiter.hasNext()) {
        val coll: MutableList<Pair<Key, Value>> = ArrayList()
        val csvrec = csviter.next()
        // does not handle the flexible case
        val bdap = Array<ByteArray>(dap.size, { pos -> csvScan.csvSchema[dap[pos]].second.encode(csvrec[dap[pos]]) })
        val brow = concatArrays(*bdap)
        val blap = Array<ByteArray>(lap.size, { pos -> csvScan.csvSchema[lap[pos]].second.encode(csvrec[lap[pos]]) })
        val bcolqPrefix = concatArrays(*blap)
        for ((cf, uap) in cap) {
          val bcf = cf.toByteArray()
          for ((name, idx) in uap) {
            val bname = name.toByteArray() // can optimize this and cf by storing the ByteArray in the beginning instead of a string
            val bval = csvScan.csvSchema[idx].second.encode(csvrec[idx])
            val bcolq = concatArrays(bcolqPrefix, bname)
            val key = Key(brow, bcf, bcolq, byteArrayOf(), Long.MAX_VALUE, false, false) // no copy
            val Val = Value(bval, false)
            coll.add(key to Val)
          }
        }
        toiter = coll.iterator()
      }
      return toiter.next()
    }

  }

}

//inline fun <reified T> concatArrays(arrs: Array<Array<T>>): Array<T> {
//  val size = arrs.sumBy { it.size }
//  val a = Array<T?>(size, {null})
//  var i = 0
//  for (arr in arrs) {
//    System.arraycopy(arr, 0, a, i, arr.size)
//    i += arr.size
//  }
//  @Suppress("UNCHECKED_CAST")
//  return a as Array<T>
//}

fun concatArrays(vararg arrs: ByteArray): ByteArray {
  val size = arrs.sumBy { it.size }
  val a = ByteArray(size)
  var i = 0
  for (arr in arrs) {
    System.arraycopy(arr, 0, a, i, arr.size)
    i += arr.size
  }
  return a
}


// class ChangeAccessPath -- destroys sort

// class RemoteStore - RemoteWriteIterator

// class

// class Sink - drop all entries - hasTop() always false