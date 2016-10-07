package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableListMultimap
import edu.washington.cs.laragraphulo.Encode
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.sample.SamplerConfiguration
import org.apache.accumulo.core.conf.AccumuloConfiguration
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Serializable
import java.net.URL
import java.util.*





class OpCSVScan(
    val url: String,
    /** Nulls in the list indicate fields to skip reading */
    val encoders: List<Encode<String>?>,
    val names: List<Pair<Name, Type<*>>>,
    val skip: Int = 0,
    val delimiter: Char = ',',
    val quote: Char = '"',
    val escape: Char? = null
) : Op<TupleIterator>(url.toObj(), encoders.toObj(), skip.toObj(), delimiter.toObj(), quote.toObj(), escape.toObj()) {
  init {
    require(encoders.filterNotNull().size == names.size) {"There must be a name/type for each (non-null) encoder. Names: $names. Encoders: $encoders"}
  }

  override val unbound: List<Arg<*>> = emptyList()

  override fun invoke(reqs: List<*>): TupleIterator {
    return CSVScan(URL(url), encoders, skip, delimiter, quote, escape)
  }
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
  private lateinit var iterator: Iterator<CSVRecord>
  private var linenumber: Int = 0

  fun init() {
    val parser = CSVParser(
        BufferedReader(InputStreamReader(url.openStream())),
        CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape))
    iterator = parser.iterator()
    for (i in 0..skip - 1) {
      iterator.next()
    }
  }

  override fun seek(seek: TupleSeekKey) {
    // start from scratch
    if (!seek.range.hasUpperBound()) {
      init()
    } else {
      // recover from a saved state
      throw UnsupportedOperationException("not implemented")
    }
  }

  override fun serializeState(): ByteArray {
    // write the line number to a bytearray
    throw UnsupportedOperationException("not implemented")
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
  override fun deepCopy(env: IteratorEnvironment): CSVScan {
    if (linenumber != 0)
      throw UnsupportedOperationException("not implemented when iteration already began")
    return CSVScan(url, encoders, skip, delimiter, quote, escape)
  }
}





data class TypedExpr(
    val expr: Expr<ABS>,
    val type: Type<*>
)

data class ValueTypedExpr(
    val name: ABS,
    val expr: Expr<FullValue>,
    val type: Type<*>
)

//
//class OpApplyIterator(
//    val source: Op<TupleIterator>,
//    val keyExprs: List<TypedExpr>,
//    val famExpr: TypedExpr,
//    val valExprs: List<ValueTypedExpr>
//) : KeySchema, ValSchema, Op<TupleIterator>(source.toObj(), keyExprs.toObj(), famExpr.toObj(), valExprs.toObj()) {
//
//  override val keyNames: List<String>
//  override val valNames: List<String>
//
//  val encodings: Map<Name,Type<*>>
//  val positions: List<Name>
//
//  init {
//    val keyNames: Map<Name, Type<*>> = keyExprs.zip(keySchema.keyNames).map { it.second to it.first.type }.toMap()
//    val valNames = valExprs.map { it.name.toString() to it.type }
//    val famName = mapOf(__FAMILY__ to famExpr.type)
//    encodings = keyNames + valNames.toMap() + famName
//    positions = keySchema.keyNames + __FAMILY__ + famName.map { it.key }
//  }
//
//
////  override val typeSchema: TypeSchema = object : TypeSchema {
////    override val types: Map<Name, Type<*>> = this@OpApplyIterator.encodings
////  }
////  override val reducingSchema: ReducingSchema = source.reducingSchema
////  override val positionSchema: PositionSchema = positions
//
//  override val unbound: List<Arg<*>> = source.unbound
//
//  override fun invoke(reqs: List<*>): TupleIterator {
//    return ApplyIterator(
//        source(reqs), keyExprs.map { it.expr }, famExpr.expr,
//        valExprs.map { it.name to it.expr }
//    )
//  }
//}







/** A [TupleIterator] that creates new tuples
 * whose keys are the result of [keyExprs],
 * family is the result of [famExpr],
 * and values are the result of [valExprs]. */
class ApplyIterator(
    val parent: TupleIterator,
    val keyExprs: List<Expr<ABS>>,
    val famExpr: Expr<ABS>,
    val valExprs: List<Pair<ABS,Expr<FullValue>>>
) : TupleIterator {
  var topTuple: Tuple? = null

//  companion object {
//    val RESULT = ABS("result".toByteArray())
//    val SRC = ABS("src".toByteArray())
//    val DST = ABS("dst".toByteArray())
//    val UNDER = '_'.toByte()
//  }

  override fun seek(seek: TupleSeekKey) {
    parent.seek(seek)
  }

  private fun applyToTuple(pt: Tuple): Tuple {
    val input = listOf(pt) // single tuple expression
    val keys = keyExprs.map { it(input) }
    val fam = famExpr(input)
    val vals: ImmutableListMultimap<ArrayByteSequence, FullValue> =
        valExprs.fold(ImmutableListMultimap.builder<ABS,FullValue>()) {
          builder, it -> builder.put(it.first, it.second(input))
        }.build()
    return TupleImpl(keys, fam, vals)
  }

  private fun prepTop() {
    if (topTuple != null && parent.hasNext()) {
      val t = parent.peek()
      // src_dst
//      val src = t.vals[SRC]!!.first().value
//      val dst = t.vals[DST]!!.first().value
//      val result = ByteArray(src.length()+dst.length()+1)
//      System.arraycopy(src.backingArray, src.offset(), result, 0, src.length())
//      result[src.length()] = UNDER
//      System.arraycopy(dst.backingArray, dst.offset(), result, src.length()+1, dst.length())
//      topTuple = TupleImpl(t.keys, t.family,
//          ImmutableListMultimap.of(RESULT, FullValue(ABS(result), EMPTY, Long.MAX_VALUE)))
      // todo - this implementation restricts Apply to return a single Tuple per input Tuple. Does not allow flatmap/ext.
      topTuple = applyToTuple(t)
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
    return ApplyIterator(parent.deepCopy(env), keyExprs, famExpr, valExprs)
  }
}





class OpRWI(
    val input: Op<SKVI>,
    val tableName: String,
    val accumuloConfig: AccumuloConfig
) : Op<SKVI>(input, Obj(tableName), Obj(accumuloConfig)) {


  override val unbound: List<Arg<*>> = input.unbound

  override fun invoke(reqs: List<*>): SortedKeyValueIterator<Key, Value> {
    val opts = accumuloConfig.basicRemoteOpts("", tableName, null, null)
    val skvi = RemoteWriteIterator()
    skvi.init(input(reqs), opts, object : IteratorEnvironment {
      override fun getAuthorizations(): Authorizations {
        throw UnsupportedOperationException("not implemented")
      }

      override fun cloneWithSamplingEnabled(): IteratorEnvironment {
        throw UnsupportedOperationException("not implemented")
      }

      override fun reserveMapFileReader(mapFileName: String?): SortedKeyValueIterator<Key, Value> {
        throw UnsupportedOperationException("not implemented")
      }

      override fun getIteratorScope(): IteratorUtil.IteratorScope {
        return IteratorUtil.IteratorScope.scan
      }

      override fun getConfig(): AccumuloConfiguration {
        throw UnsupportedOperationException("not implemented")
      }

      override fun isSamplingEnabled(): Boolean {
        throw UnsupportedOperationException("not implemented")
      }

      override fun isFullMajorCompaction(): Boolean {
        throw UnsupportedOperationException("not implemented")
      }

      override fun getSamplerConfiguration(): SamplerConfiguration {
        throw UnsupportedOperationException("not implemented")
      }

      override fun registerSideChannel(iter: SortedKeyValueIterator<Key, Value>?) {
        throw UnsupportedOperationException("not implemented")
      }
    })
    return skvi
  }

  companion object {
    /**
     * This method shouldn't really be public, but it is useful for setting up some of the iterators.

     * Create the basic iterator settings for the [RemoteWriteIterator].
     * @param prefix A prefix to apply to keys in the option map, e.g., the "B" in "B.tableName".
     * @param remoteTable Name of table to write to. Null does not put in the table name.
     * @param remoteTableTranspose Name of table to write transpose to. Null does not put in the transpose table name.
     * @param authorizations Authorizations for the server-side iterator. Null means use default: Authorizations.EMPTY
     * @return The basic set of options for [RemoteWriteIterator].
     */
    fun AccumuloConfig.basicRemoteOpts(prefix: String?, remoteTable: String?,
                        remoteTableTranspose: String?, authorizations: Authorizations?): Map<String, String> {
      var prefix = prefix
      if (prefix == null) prefix = ""
      val opt = HashMap<String, String>()
      val instance = connector.instance.instanceName
      val zookeepers = connector.instance.zooKeepers
      val user = connector.whoami()
      opt.put(prefix + RemoteSourceIterator.ZOOKEEPERHOST, zookeepers)
      opt.put(prefix + RemoteSourceIterator.INSTANCENAME, instance)
      if (remoteTable != null)
        opt.put(prefix + RemoteSourceIterator.TABLENAME, remoteTable)
      if (remoteTableTranspose != null)
        opt.put(prefix + RemoteWriteIterator.TABLENAMETRANSPOSE, remoteTableTranspose)
      opt.put(prefix + RemoteSourceIterator.USERNAME, user)
      opt.put(prefix + RemoteSourceIterator.AUTHENTICATION_TOKEN, SerializationUtil.serializeWritableBase64(authenticationToken))
      opt.put(prefix + RemoteSourceIterator.AUTHENTICATION_TOKEN_CLASS, authenticationToken.javaClass.name)
      if (authorizations != null && authorizations != Authorizations.EMPTY)
        opt.put(prefix + RemoteSourceIterator.AUTHORIZATIONS, authorizations.serialize())
      return opt
    }
  }
}

//class StoreIterator(
//    val tableName: String,
//    val accumuloConfig: AccumuloConfig
//) : SKVI {
//  lateinit var writer: BatchWriter
//
//  fun setup() {
//    val bwc = BatchWriterConfig()
//    writer = accumuloConfig.connector.createBatchWriter(tableName, bwc)
//
//  }
//
//  override fun init(source: SortedKeyValueIterator<Key, Value>?, options: MutableMap<String, String>?, env: IteratorEnvironment?) {
//
//  }
//
//  override fun next() {
//    throw UnsupportedOperationException("not implemented")
//  }
//
//  override fun seek(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
//    throw UnsupportedOperationException("not implemented")
//  }
//
//  override fun getTopValue(): Value {
//    throw UnsupportedOperationException("not implemented")
//  }
//
//  override fun hasTop(): Boolean {
//    throw UnsupportedOperationException("not implemented")
//  }
//
//  override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key, Value> {
//    throw UnsupportedOperationException("not implemented")
//  }
//
//  override fun getTopKey(): Key {
//    throw UnsupportedOperationException("not implemented")
//  }
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