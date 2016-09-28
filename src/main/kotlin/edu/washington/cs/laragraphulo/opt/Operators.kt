package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableListMultimap
import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URL
import java.util.*

/**
 * An attribute name.
 */
typealias Name = String
typealias CfName = Name


class CSVScan(val url: Op<URL>,
              val accessPath: Op<ImmutableAccessPath>,
              /**
               * For each csv column, define an attribute name and a conversion from String to byte[]
               */
              val csvSchema: Op<List<Pair<Name, LexicoderPlus<String>>>>,
              val delimiter: Op<Char> = Obj(',')
) : Op<IteratorFlow>(url, accessPath) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}

private class CSVScan_impl(val url: URL,
                           val accessPath: ImmutableAccessPath,
                           /**
                            * For each csv column, define an attribute name and a conversion from String to byte[]
                            */
                           val csvSchema: List<Pair<Name, LexicoderPlus<String>>>,
                           val delimiter: Char = ','
)


/**
 * The output schema places all attributes into the key attributes, in the order of the encoders.
 */
class CSVSc(
    val url: URL,
    val encoders: List<LexicoderPlus<String>>,
    val skip: Int = 0,
    val delimiter: Char = ',',
    val quote: Char = '"',
    val escape: Char? = null
) : TupleIterator {
//  val parser: CSVParser
  val iterator: Iterator<CSVRecord>
  var linenumber: Int = 0

  init {
    val parser = CSVParser(
        BufferedReader(InputStreamReader(url.openStream())),
        CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape))
    iterator = parser.iterator()
    for (i in 0..skip - 1) {
      iterator.next()
    }
  }

  var top: Tuple? = null

  private fun findTop() {
    if (top == null && iterator.hasNext()) {
      val csvRecord = iterator.next()
      if (csvRecord.size() != encoders.size) {
        throw RuntimeException("error parsing line $linenumber: expected ${encoders.size} attributes: $csvRecord")
      }
      val attrs = csvRecord.zip(encoders).map { ArrayByteSequence(it.second.encode(it.first)) }
      top = TupleImpl(attrs, EMPTY, ImmutableListMultimap.of())
      linenumber++
    }
  }

  override fun hasNext(): Boolean {
    findTop()
    return top != null
  }

  override fun next(): Tuple {
    findTop()
    val t = top ?: throw NoSuchElementException()
    top = null
    return t
  }

  override fun peek(): Tuple {
    findTop()
    return top ?: throw NoSuchElementException()
  }

  override fun seek(sk: TupleSeekKey) {
    // recover from a saved state
    throw UnsupportedOperationException("not implemented")
  }

  override fun serializeState(): ByteArray {
    // write the line number to a bytearray
    throw UnsupportedOperationException("not implemented")
  }

  override fun deepCopy(env: IteratorEnvironment): CSVSc {
    if (linenumber != 0)
      throw UnsupportedOperationException("not implemented when iteration already began")
    return CSVSc(url, encoders, skip, delimiter, quote, escape)
  }
}


//private fun doit(csvScan: CSVScan_impl): Iterator<Pair<Key, Value>> {
////  val encodeds = Array<ByteArray?>(csvScan.accessPath.allAttributes.size, {null})
//  val parser = CSVParser(BufferedReader(InputStreamReader(csvScan.url.openStream())),
//      CSVFormat.newFormat(csvScan.delimiter))
//  val dap: List<Int> = csvScan.accessPath.dap.map { attr -> csvScan.csvSchema.indexOfFirst { attr == it.first } }
//  val lap: List<Int> = csvScan.accessPath.lap.map { attr -> csvScan.csvSchema.indexOfFirst { attr == it.first } }
//  val cap: List<Pair<CfName, List<Pair<Name,Int>>>> = csvScan.accessPath.cap.map { cfpair -> cfpair.name to cfpair.attributes.map { name -> name to csvScan.csvSchema.indexOfFirst { name == it.first } }}
//
//  return object: Iterator<Pair<Key, Value>> {
//
//    val csviter = parser.iterator()
//    var toiter: Iterator<Pair<Key, Value>> = Collections.emptyIterator()
//
//    override fun hasNext(): Boolean {
//      return toiter.hasNext() || csviter.hasNext()
//    }
//
//    override fun next(): Pair<Key, Value> {
//      if (!toiter.hasNext()) {
//        val coll: MutableList<Pair<Key, Value>> = ArrayList()
//        val csvrec = csviter.next()
//        // does not handle the flexible case
//        val bdap = Array<ByteArray>(dap.size, { pos -> csvScan.csvSchema[dap[pos]].second.encode(csvrec[dap[pos]]) })
//        val brow = concatArrays(*bdap)
//        val blap = Array<ByteArray>(lap.size, { pos -> csvScan.csvSchema[lap[pos]].second.encode(csvrec[lap[pos]]) })
//        val bcolqPrefix = concatArrays(*blap)
//        for ((cf, uap) in cap) {
//          val bcf = cf.toByteArray()
//          for ((name, idx) in uap) {
//            val bname = name.toByteArray() // can optimize this and cf by storing the ByteArray in the beginning instead of a string
//            val bval = csvScan.csvSchema[idx].second.encode(csvrec[idx])
//            val bcolq = concatArrays(bcolqPrefix, bname)
//            val key = Key(brow, bcf, bcolq, byteArrayOf(), Long.MAX_VALUE, false, false) // no copy
//            val Val = Value(bval, false)
//            coll.add(key to Val)
//          }
//        }
//        toiter = coll.iterator()
//      }
//      return toiter.next()
//    }
//
//  }
//
//}

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