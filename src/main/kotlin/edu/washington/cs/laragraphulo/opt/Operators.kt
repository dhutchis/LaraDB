package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableListMultimap
import edu.washington.cs.laragraphulo.Encode
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Serializable
import java.net.URL
import java.util.*



sealed class AccumuloOp(args: List<Op<*>> = emptyList()) : Op<Tuple>(args), Serializable
{
  constructor(vararg args: Op<*>): this(args.asList())

  /**
   * From the RACO types in the catalog for Scan; FileScan scheme.
   */
  abstract val encodingSchema: EncodingSchema
  /**
   * todo. Attach a prior iterator in place of this.
   */
  abstract val reducingSchema: ReducingSchema
  /**
   * From the order of the attributes in the catalog / FileScan scheme; with placeholders __DAP__ and __LAP__
   */
  abstract val keySchema: KeySchema
  /**
   * From the order of the attributes in the catalog / FileScan scheme; with placeholders __DAP__ and __LAP__
   */
  abstract val positionSchema: PositionSchema

  /**
   * @param parent The source iterator that one of the leaves will connect to.
   * @param options Execution-time environmental parameters, passed from client
   * @param env Execution-time Accumulo parameters
   */
  abstract fun construct(parent: AccumuloLikeIterator<*,*>, options: Map<String,String>, env: IteratorEnvironment): AccumuloLikeIterator<*,*>
}




class OpCSVScan(
    val url: Obj<String>,
    /** Nulls in the list indicate fields to skip reading */
    val encoders: Obj<List<Encode<String>?>>,
    val names: Obj<List<Pair<Name,Type<*>>>>,
    val skip: Obj<Int> = Obj(0),
    val delimiter: Obj<Char> = Obj(','),
    val quote: Obj<Char> = Obj('"'),
    val escape: Obj<Char?> = Obj(null)
) : AccumuloOp(url, encoders, skip, delimiter, quote, escape) {
  init {
    require(encoders().filterNotNull().size == names().size) {"There must be a name/type for each (non-null) encoder. Names: $names. Encoders: $encoders"}
  }
  override fun construct(parent: AccumuloLikeIterator<*, *>, options: Map<String, String>, env: IteratorEnvironment): CSVScan {
    return CSVScan(URL(url.obj), encoders.obj, skip.obj, delimiter.obj, quote.obj, escape.obj)
  }

  override val encodingSchema = object : EncodingSchema {
    override val encodings: Map<Name, Type<*>> = names().toMap()
  }
  override val reducingSchema = object : ReducingSchema {
    override val reducers: Map<Name, (List<FullValue>) -> FullValue> = emptyMap()
  }
  override val keySchema = object : KeySchema {
    override val keyNames: List<Name> = names().map { it.first }
  }
  override val positionSchema: List<Name> = names().map { it.first }
}


/**
 * The output schema places all attributes into the key attributes, in the order of the encoders.
 */
class CSVScan(
    val url: URL,
    /** Nulls in the list indicate fields to skip reading */
    val encoders: List<Encode<String>?>,
    val skip: Int = 0,
    val delimiter: Char = ',',
    val quote: Char = '"',
    val escape: Char? = null
) : TupleIterator {
//  val parser: CSVParser
  private val iterator: Iterator<CSVRecord>
  private var linenumber: Int = 0

  init {
    val parser = CSVParser(
        BufferedReader(InputStreamReader(url.openStream())),
        CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape))
    iterator = parser.iterator()
    for (i in 0..skip - 1) {
      iterator.next()
    }
  }

  private var top: Tuple? = null

  private fun findTop() {
    if (top == null && iterator.hasNext()) {
      val csvRecord = iterator.next()
      if (csvRecord.size() != encoders.size) {
        throw RuntimeException("error parsing line $linenumber: expected ${encoders.size} attributes: $csvRecord")
      }
      val attrs = csvRecord.zip(encoders).filter { it.second != null }.map { ArrayByteSequence(it.second!!.encode(it.first)) }
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

  override fun deepCopy(env: IteratorEnvironment): CSVScan {
    if (linenumber != 0)
      throw UnsupportedOperationException("not implemented when iteration already began")
    return CSVScan(url, encoders, skip, delimiter, quote, escape)
  }
}
















/** Mock version that does a fixed thing. */
class ApplyIterator(
    val parent: TupleIterator
) : TupleIterator {
  var topTuple: Tuple? = null

  companion object {
    val RESULT = ArrayByteSequence("result".toByteArray())
    val SRC = ArrayByteSequence("src".toByteArray())
    val DST = ArrayByteSequence("dst".toByteArray())
    val UNDER = '_'.toByte()
  }

  override fun seek(sk: TupleSeekKey) {
    parent.seek(sk)
  }

  private fun prepTop() {
    if (topTuple != null && parent.hasNext()) {
      val t = parent.peek()
      // src_dst
      val src = t.vals[SRC]!!.first().value
      val dst = t.vals[DST]!!.first().value
      val result = ByteArray(src.length()+dst.length()+1)
      System.arraycopy(src.backingArray, src.offset(), result, 0, src.length())
      result[src.length()] = UNDER
      System.arraycopy(dst.backingArray, dst.offset(), result, src.length()+1, dst.length())
      topTuple = TupleImpl(t.keys, t.family,
          ImmutableListMultimap.of(RESULT, FullValue(ArrayByteSequence(result), EMPTY, Long.MAX_VALUE)))
    }
  }

  override fun peek(): Tuple {
    prepTop()
    return topTuple!!
  }

  override fun next(): Tuple {
    prepTop()
    val t = topTuple!!
    parent.next()
    topTuple = null
    return t
  }

  override fun hasNext(): Boolean {
    return parent.hasNext()
  }

  override fun serializeState(): ByteArray {
    throw UnsupportedOperationException("not implemented")
  }

  override fun deepCopy(env: IteratorEnvironment): ApplyIterator {
    return ApplyIterator(parent.deepCopy(env))
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