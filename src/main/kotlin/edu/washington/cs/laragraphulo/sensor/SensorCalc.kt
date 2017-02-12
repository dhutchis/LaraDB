package edu.washington.cs.laragraphulo.sensor

import com.google.common.collect.Iterators
import edu.mit.ll.graphulo.DynamicIteratorSetting
import edu.mit.ll.graphulo.Graphulo
import edu.mit.ll.graphulo.apply.ApplyIterator
import edu.mit.ll.graphulo.apply.ApplyOp
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply
import edu.mit.ll.graphulo.ewise.EWiseOp
import edu.mit.ll.graphulo.reducer.ReducerSerializable
import edu.mit.ll.graphulo.rowmult.MultiplyOp
import edu.mit.ll.graphulo.skvi.DoubleCombiner
import edu.mit.ll.graphulo.skvi.DoubleSummingCombiner
import edu.mit.ll.graphulo.skvi.TwoTableIterator
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder
import org.apache.accumulo.core.client.lexicoder.PairLexicoder
import org.apache.accumulo.core.client.lexicoder.ULongLexicoder
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.*
import org.apache.accumulo.core.util.ComparablePair
import org.apache.accumulo.core.util.format.DefaultFormatter
import org.apache.accumulo.core.util.format.FormatterConfig
import org.apache.hadoop.io.Text
import java.io.IOException
import java.util.*


private val ullex = ULongLexicoder()
private val dlex = DoubleLexicoder()
fun ByteArray.toLong(encode: Boolean): Long = if (encode) ullex.decode(this) else String(this).toLong()
fun Long.toByteArray(encode: Boolean): ByteArray = if (encode) ullex.encode(this) else this.toString().toByteArray()
fun ByteArray.toDouble(encode: Boolean): Double = if (encode) dlex.decode(this) else String(this).toDouble()
fun Double.toByteArray(encode: Boolean): ByteArray = if (encode) dlex.encode(this) else this.toString().toByteArray()




fun Set<SensorCalc.SensorOpt>.printSet() = this.sorted().joinToString("") { it.rep.toString() }
fun Set<SensorCalc.SensorOpt>.printSetAll() =
    SensorCalc.SensorOpt.values().joinToString("") { if (it in this) it.rep.toString() else " " }

private data class TimedResult<out R>(
    val result: R,
    val time: Double
)
private inline fun <R> time(f: () -> R): TimedResult<R> {
  val start = System.currentTimeMillis()
  val result = f()
  val time = (System.currentTimeMillis() - start)/1000.0
  return TimedResult(result, time)
}



class SensorCalc(
    val conn: Connector,
    val pw: PasswordToken,
    val sensorA: String,
    val sensorB: String,
    val opts: Set<SensorOpt>
) {
  val sensorA2 = "${sensorA}_newt"
  val sensorB2 = "${sensorB}_newt"
  val sensorX = "${sensorA}_${sensorB}_X"
  val sensorU = "${sensorA}_${sensorB}_U"
  val sensorC = "${sensorA}_${sensorB}_C"
  val G by lazy { Graphulo(conn, pw) }
  val sensorM = "${sensorA}_${sensorB}_M"
  val sensorF = "${sensorA}_${sensorB}_F"
  
  enum class SensorOpt(
      val rep: Char
  ) : Comparable<SensorOpt> {
    AggregatePush('A'),          //
    //    ClientScalar,          //
    Defer('D'),                  //
    Encode('E'),                 // ok
    FilterPush('F'),             // ok
    MonotoneSortElim('M'),       // ok, in serial mode
    PropagatePartition('P'),     //
    ReuseSource('R'),            //
    SymmetricCovariance('S'),    //
    ZeroDiscard('Z');            // ok
    //    override fun compareTo(other: SensorOpt): Int = this.rep.compareTo(other.rep)
    companion object {
      val fromRep: Map<Char,SensorOpt> = SensorOpt.values().map { it.rep to it }.toMap()
      val num = values().size
    }
  }
  private val AggregatePush = SensorOpt.AggregatePush
  private val ZeroDiscard = SensorOpt.ZeroDiscard
  private val Defer = SensorOpt.Defer
  private val Encode = SensorOpt.Encode
  private val FilterPush = SensorOpt.FilterPush
  private val MonotoneSortElim = SensorOpt.MonotoneSortElim
  private val PropagatePartition = SensorOpt.PropagatePartition
  private val ReuseSource = SensorOpt.ReuseSource
  private val SymmetricCovariance = SensorOpt.SymmetricCovariance

  companion object {
    private val ull = ULongLexicoder()
  }


  /** Time to run the SensorCalc pipeline to X, to U, to C.
   * No overlap; times are additive. Does not include table creation time. */
  data class SensorCalcTimes(
      val opts: Set<SensorOpt>,
      val toX: Double,
      val toU: Double,
      val toC: Double
  ) {
    override fun toString(): String {
      return "${opts.printSetAll()},$toX,$toU,$toC"
    }
  }


  
  fun timeAll(minTime: Long, maxTime: Long): SensorCalcTimes {
    _pre_binAndDiff()
    _pre_meanAndSubtract()
    _pre_covariance()
    val (tCount, tX) = time { _binAndDiff(minTime, maxTime) }
    val (_, tU) = time { _meanAndSubtract() }
    val (_, tC) = time { _covariance(tCount) }

    return SensorCalcTimes(opts, tX, tU, tC)
  }



  private fun recreate(vararg tns: String) {
    tns.forEach { tn ->
      if (conn.tableOperations().exists(tn))
        conn.tableOperations().delete(tn)
      conn.tableOperations().create(tn)
    }
  }

  private val AverageValuesByRow = DynamicIteratorSetting(21, "avgByRow")
      .append(AppendCounterApply.iteratorSetting(1, Encode in opts))
      .append(CombineSumCnt.iteratorSetting(1, Encode in opts))
      .append(DividePairApply.iteratorSetting(1, Encode in opts))
      .toIteratorSetting()

  fun binAndDiff(minTime: Long, maxTime: Long): Long {
    _pre_binAndDiff()
    return _binAndDiff(minTime, maxTime)
  }
  fun _pre_binAndDiff() {
    require(conn.tableOperations().exists(sensorA)) {"table $sensorA does not exist"}
    require(conn.tableOperations().exists(sensorB)) {"table $sensorB does not exist"}
    if (MonotoneSortElim !in opts)
      recreate(sensorA2, sensorB2)
    recreate(sensorX)
  }
  private fun _binAndDiff(minTime: Long, maxTime: Long): Long {
    val rowFilter: String?
    if (FilterPush in opts) {
      val minRow = (if (Encode !in opts) minTime else Value(ull.encode(minTime))).toString()
      val maxRow = (if (Encode !in opts) maxTime else Value(ull.encode(maxTime))).toString()
      if (Encode in opts && (minRow.contains(':') || maxRow.contains(':')))
        throw RuntimeException("Special character detected ':' in time string for row filter")
      var i = 0
      while (minRow.contains(i.toChar()) || maxRow.contains(i.toChar()))
        i++
      val sep = i.toChar()
      rowFilter = "$minRow$sep:$sep$maxRow$sep"
    } else {
      rowFilter = null
    }

    val itersBefore: List<IteratorSetting> = {
      val dis = DynamicIteratorSetting(21, "filterBinAvg")
          .append(BinRowApply.iteratorSetting(1, Encode in opts))
          .append(AverageValuesByRow)
      if (FilterPush !in opts)
        dis.prepend(MinMaxFilter.iteratorSetting(1, minTime, maxTime, Encode in opts))
      dis.iteratorSettingList
    }()
    val tCounter = RowCountReduce()
//    tCounter.init(emptyMap<String,String>().toMutableMap(), null)

    val subtract = SubtractEWise::class.java
    val subtractOptions = SubtractEWise.optionMap(Encode in opts, keep_zero = true)

    if (MonotoneSortElim in opts) {
      G.TwoTableEWISE(sensorA, sensorB, null, sensorX, // transpose to [c,t']
          -1, subtract, subtractOptions,
          null, rowFilter, null, null, false, false,
          itersBefore, itersBefore, null, tCounter, null, -1, null, null)
    } else {
      // 2 pass version, slowed down because sort elim is disabled
      val dis = DynamicIteratorSetting(21, "filterBin")
      if (FilterPush !in opts)
        dis.append(MinMaxFilter.iteratorSetting(1, minTime, maxTime, Encode in opts))
      dis.append(BinRowApply.iteratorSetting(1, Encode in opts))
      val iters = dis.iteratorSettingList
      G.OneTable(sensorA, sensorA2, null,
          null, -1, null, null, null,
          rowFilter, null, iters, null, null)
      G.OneTable(sensorB, sensorB2, null,
          null, -1, null, null, null,
          rowFilter, null, iters, null, null)

      G.TwoTableEWISE(sensorA2, sensorB2, null, sensorX, // transpose to [c,t']
          -1, subtract, subtractOptions,
          null, rowFilter, null, null, false, false,
          listOf(AverageValuesByRow), listOf(AverageValuesByRow), null, tCounter, null, -1, null, null)
    }

    val tCount = tCounter.serializableForClient
    println("tCount is $tCount")
    return tCount
  }

  fun meanAndSubtract() {
    _pre_meanAndSubtract()
    _meanAndSubtract()
  }
  fun _pre_meanAndSubtract() {
    require(conn.tableOperations().exists(sensorX)) {"table $sensorX does not exist"}
    recreate(sensorU)
  }
  fun _meanAndSubtract() {
    val leftIters = DynamicIteratorSetting(21, "avgLeftByRow")
        .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))
        .append(AverageValuesByRow)
        .iteratorSettingList
//    , IteratorSetting(1, DebugInfoIterator::class.java))
    val keepZeros = ZeroDiscard !in opts

    G.TwoTableROWCartesian(TwoTableIterator.CLONESOURCE_TABLENAME, sensorX, null, sensorU, // transpose to [t',c]
        -1, MinusRowEwiseRight::class.java, MinusRowEwiseRight.optionMap(Encode in opts, keep_zero = keepZeros),
        null, null, null, null, false, false, false, false, false, false,
        leftIters, null, null, null, null, -1, null, null)

  }

  /**
   * Matrix inner product.
   * Divide by tCount - 1 in the end.
   */
  fun covariance(tCount: Long) {
    _pre_covariance()
    _covariance(tCount)
  }
  fun _pre_covariance() {
//    require(tCount > 1) {"Bad tCount: $tCount"}
    require(conn.tableOperations().exists(sensorU)) {"table $sensorU does not exist"}
    recreate(sensorC)
  }
  fun _covariance(tCount: Long) {
    val plusIter = IteratorSetting(Graphulo.DEFAULT_COMBINER_PRIORITY, DoubleSummingCombiner::class.java)
    DoubleSummingCombiner.setEncodingType(plusIter, if (Encode !in opts) DoubleCombiner.Type.STRING else DoubleCombiner.Type.BYTE)
    DoubleSummingCombiner.setCombineAllColumns(plusIter, true)
    val keepZeros = ZeroDiscard !in opts

    G.TableMult(TwoTableIterator.CLONESOURCE_TABLENAME, sensorU, sensorC, null,
        -1, Multiply::class.java, Multiply.optionMap(Encode in opts, keep_zero = keepZeros), // drop zero
        plusIter, // discards zeros
        null, null, null, false, false,
        null, null, null,
        null, null, -1, null, null)

    GraphuloUtil.applyIteratorSoft(
        GraphuloUtil.addOnScopeOption(
            DivideApply.iteratorSetting(Graphulo.DEFAULT_COMBINER_PRIORITY+1, Encode in opts, (tCount-1).toDouble(), keep_zero = keepZeros),
            EnumSet.of(IteratorUtil.IteratorScope.scan)),
        conn.tableOperations(), sensorC)
  }

}




/**
 * The row contains the measurement time.
 * Bin to the nearest 60 seconds (=60000 ms) according to:
 *
 * > def $\operatorname{bin}(t)=t-\operatorname{mod}(t,60)+60 \left\lfloor \frac{\operatorname{mod}(t,60)}{60}+.5 \right\rfloor$
 */
class BinRowApply : ApplyOp {
  /** Whether to use string encoding or to use ulong encoding. */
  private var encode: Boolean = false

  val binSize = 60000

  companion object {
    const val ENCODE = "ENCODE"

    fun iteratorSetting(priority: Int, encode: Boolean = false): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, BinRowApply::class.java.name)
      if (encode)
        itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + ENCODE, encode.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    val t = k.rowData.toArray().toLong(encode)
    val tm = t % binSize
    val tb = t - tm + (if (tm >= binSize/2) binSize else 0) // new t
    val rb = tb.toByteArray(encode) // new row
    val kb = Key(rb, k.columnFamilyData.toArray(), k.columnQualifierData.toArray(), k.columnVisibility.bytes) // no ts
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(kb,v))
  }
}

/**
 * Append ",1" or a ULong 1 to the Value.
 * Used before combining sum,cnt toward computing average.
 */
class AppendCounterApply : ApplyOp {
  /** Whether to use string encoding or to use ulong encoding. */
  private var encode: Boolean = false

  companion object {
    const val ENCODE = "ENCODE"
    private val dlex = DoubleLexicoder()
    private val lex = PairLexicoder(DoubleLexicoder(), ULongLexicoder())
    private val commaOne = ",1".toByteArray()

    fun iteratorSetting(priority: Int, encode: Boolean = false): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, AppendCounterApply::class.java.name)
      if (encode)
        itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + ENCODE, encode.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    val nvarr = if (!encode) {
      val varr = v.get()
      val sz = varr.size
      val nvarr = Arrays.copyOf(varr, sz+commaOne.size)
      System.arraycopy(commaOne, 0, nvarr, sz, commaOne.size)
      nvarr
    } else {
      lex.encode(ComparablePair(dlex.decode(v.get()), 1L))
    }
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(k,Value(nvarr)))
  }
}

/**
 * Combine via sum and cnt. Interpret as Double, with count as ULong.
 * When encoding as String, must be of the form "5.6,3".
 * When encoding as numbers, must be of the form Pair<Double,ULong>
 * Used for computing average.
 */
class CombineSumCnt : Combiner() {
  /** Whether to use string encoding or to use numeric encoding. */
  private var encode: Boolean = false

  override fun reduce(key: Key, iter: Iterator<Value>): Value {
    var sum = 0.0
    var count: Long = 0L

    if (!iter.hasNext()) return Value()
    var next: Value? = iter.next()
    if (!iter.hasNext()) return next!!

    do {
      if (!encode) {
        val stats = next!!.toString().split(',') //.dropLastWhile(String::isEmpty).toTypedArray()
        require(stats.size == 2) {"bad value: $next"}
        sum += stats[0].toDouble()
        count += stats[1].toLong()
      } else {
        val pair = lex.decode(next!!.get())
        sum += pair.first
        count += pair.second
      }
      next = if (iter.hasNext()) iter.next() else null
    } while (next != null)

    val ret = if (!encode) "$sum,$count".toByteArray() else lex.encode(ComparablePair(sum, count))
    return Value(ret)
  }

  @Throws(IOException::class)
  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment?) {
    super.init(source, options, env)
    encode = options[ENCODE]?.toBoolean() ?: encode
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    val io = super.describeOptions()
    io.setName("CombineSumCnt")
    io.setDescription("Combiner that keeps track of sum and count of pairs of doubles and longs")
    io.addNamedOption(ENCODE, "True for pair lexicoding, false for string encoding (default false)")
    return io
  }

  companion object {
    const val ENCODE = "ENCODE"
    private val lex = PairLexicoder(DoubleLexicoder(), ULongLexicoder())

    fun iteratorSetting(priority: Int, encode: Boolean = false,
                        columns: List<IteratorSetting.Column> = emptyList()): IteratorSetting {
      val itset = IteratorSetting(priority, CombineSumCnt::class.java)
      if (columns.isEmpty()) Combiner.setCombineAllColumns(itset, true)
      else Combiner.setColumns(itset, columns)
      if (encode) itset.addOption(ENCODE, encode.toString())
      return itset
    }
  }

}


/**
 * Divides value interpreted as a pair, `first / second`.
 */
class DividePairApply : ApplyOp {
  /** Whether to use string encoding or to use numeric encoding. */
  private var encode: Boolean = false

  companion object {
    const val ENCODE = "ENCODE"
    private val dlex = DoubleLexicoder()
    private val lex = PairLexicoder(DoubleLexicoder(), ULongLexicoder())

    fun iteratorSetting(priority: Int, encode: Boolean): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, DividePairApply::class.java.name)
      if (encode)
        itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + ENCODE, encode.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    if (!encode) {
      val stats = v.toString().split(',')
      require(stats.size == 2) {"bad value: $v"}
      val sum = stats[0].toDouble()
      val cnt = stats[1].toLong()
      val nv = (sum / cnt).toString().toByteArray()
      return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(k,Value(nv)))
    } else {
      val pair = lex.decode(v.get())
      val sum = pair.first
      val cnt = pair.second
      val nv = dlex.encode(sum / cnt)
      return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(k,Value(nv)))
    }
  }
}






/**
 * Count the unique rows. Assumes rows given in order.
 */
class RowCountReduce : ReducerSerializable<Long>() {
  var count = 0L
  var lastRow = Text()

  override fun combine(another: Long) {
    count += another
  }

  override fun getSerializableForClient(): Long = count

  override fun hasTopForClient(): Boolean = count != 0L

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
  }

  override fun reset() {
    count = 0
  }

  override fun update(k: Key, v: Value) {
    if (k.compareRow(lastRow) == 0) return
    count++
    lastRow = k.getRow(lastRow)
  }
}


/**
 * Use for a MultiplyOp.
 * Aligns on row, result has row = common row, colQ = right colQ, val = left - right
 */
class MinusRowEwiseRight : MultiplyOp {
  /** Whether to use string encoding or to use numeric encoding. */
  private var encode: Boolean = false
  /** Whether to discard or retain values that are zero after subtraction. */
  private var keep_zero: Boolean = true

  companion object {
    const val ENCODE = "ENCODE"
    const val KEEP_ZERO = "KEEP_ZERO"

    fun optionMap(encode: Boolean = false, keep_zero: Boolean = true): Map<String, String> {
      val map = HashMap<String, String>()
      if (encode) map.put(ENCODE, encode.toString())
      if (!keep_zero) map.put(KEEP_ZERO, keep_zero.toString())
      return map
    }
  }


  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
    keep_zero = options[KEEP_ZERO]?.toBoolean() ?: keep_zero
  }

  override fun multiply(Mrow: ByteSequence,
                        ATcolF: ByteSequence, ATcolQ: ByteSequence, ATcolVis: ByteSequence, ATtime: Long,
                        BcolF: ByteSequence, BcolQ: ByteSequence, BcolVis: ByteSequence, Btime: Long,
                        ATval: Value, Bval: Value): Iterator<Map.Entry<Key, Value>> {
    if (!keep_zero && ATval == Bval) return Collections.emptyIterator()
    val a = ATval.get().toDouble(encode)
    val b = Bval.get().toDouble(encode)
    val r = a - b
    if (!keep_zero && r == 0.0) return Collections.emptyIterator()
    val nv = Value(r.toByteArray(encode))
    val k = Key(Mrow.toArray(), ATcolF.toArray(), BcolQ.toArray())
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry<Key, Value>(k, nv))
  }
}





/**
 * Use for a MultiplyOp.
 * Aligns on row, result has row = common row, colQ = right colQ, val = left - right
 */
class SubtractEWise : EWiseOp {
  /** Whether to use string encoding or to use numeric encoding. */
  private var encode: Boolean = false
  /** Whether to discard or retain values that are zero after subtraction. */
  private var keep_zero: Boolean = true

  companion object {
    const val ENCODE = "ENCODE"
    const val KEEP_ZERO = "KEEP_ZERO"

    fun optionMap(encode: Boolean = false, keep_zero: Boolean = true): Map<String, String> {
      val map = HashMap<String, String>()
      if (encode) map.put(ENCODE, encode.toString())
      if (!keep_zero) map.put(KEEP_ZERO, keep_zero.toString())
      return map
    }
  }


  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
    keep_zero = options[KEEP_ZERO]?.toBoolean() ?: keep_zero
  }

  override fun multiply(Mrow: ByteSequence, McolF: ByteSequence, McolQ: ByteSequence, McolVis: ByteSequence,
                        Atime: Long, Btime: Long,
                        Aval: Value, Bval: Value): Iterator<Map.Entry<Key, Value>> {
    if (!keep_zero && Aval == Bval) return Collections.emptyIterator()
    val a = Aval.get().toDouble(encode)
    val b = Bval.get().toDouble(encode)
    val r = a - b
    if (!keep_zero && r == 0.0) return Collections.emptyIterator()
    val nv = Value(r.toByteArray(encode))
    val k = Key(Mrow.toArray(), McolF.toArray(), McolQ.toArray())
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry<Key, Value>(k, nv))
  }
}


/** Only exists because MathTwoScalar doesn't support direct Double encoding; it encodes by String. */
class Multiply : MultiplyOp {
  /** Whether to use string encoding or to use numeric encoding. */
  private var encode: Boolean = false
  /** Whether to discard or retain values that are zero after multiplication. */
  private var keep_zero: Boolean = true

  companion object {
    const val ENCODE = "ENCODE"
    const val KEEP_ZERO = "KEEP_ZERO"

    fun optionMap(encode: Boolean = false, keep_zero: Boolean = true): Map<String, String> {
      val map = HashMap<String, String>()
      if (encode) map.put(ENCODE, encode.toString())
      if (!keep_zero) map.put(KEEP_ZERO, keep_zero.toString())
      return map
    }
  }


  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
    keep_zero = options[KEEP_ZERO]?.toBoolean() ?: keep_zero
  }

  override fun multiply(Mrow: ByteSequence,
                        ATcolF: ByteSequence, ATcolQ: ByteSequence, ATcolVis: ByteSequence, ATtime: Long,
                        BcolF: ByteSequence, BcolQ: ByteSequence, BcolVis: ByteSequence, Btime: Long,
                        ATval: Value, Bval: Value): Iterator<Map.Entry<Key, Value>> {
    val a = ATval.get().toDouble(encode)
    val b = Bval.get().toDouble(encode)
    val r = a * b
    if (!keep_zero && r == 0.0) return Collections.emptyIterator()
    val nv = Value(r.toByteArray(encode))
    val k = Key(ATcolQ.toArray(), ATcolF.toArray(), BcolQ.toArray())
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry<Key, Value>(k, nv))
  }
}


class DivideApply : ApplyOp {
  /** Whether to use string encoding or to use numeric encoding. */
  private var encode: Boolean = false
  /** Whether to discard or retain values that are zero after multiplication. */
  private var keep_zero: Boolean = true
  private var divisor: Double = 1.0

  companion object {
    const val ENCODE = "ENCODE"
    const val DIVISOR = "DIVISOR"
    const val KEEP_ZERO = "KEEP_ZERO"

    fun iteratorSetting(priority: Int, t_string: Boolean, divisor: Double, keep_zero: Boolean = true): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, DivideApply::class.java.name)
      if (!t_string) itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + ENCODE, t_string.toString())
      if (!keep_zero) itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + KEEP_ZERO, keep_zero.toString())
      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + DIVISOR, divisor.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    encode = options[ENCODE]?.toBoolean() ?: encode
    keep_zero = options[KEEP_ZERO]?.toBoolean() ?: keep_zero
    require(options.containsKey(DIVISOR)) {"No DIVISOR given"}
    divisor = options[DIVISOR]!!.toDouble()
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    val ov = v.get().toDouble(encode)
    if (!keep_zero && ov == 0.0) return Collections.emptyIterator()
    val nv = ov / divisor
    val newval = Value(nv.toByteArray(encode))
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(k, newval))
  }
}






/**
 * Used for displaying numeric Doubles in the Accumulo shell.
 * Set `table.formatter` to `edu.washington.cs.laragraphulo.sensor.DoubleValueDisplay` on the appropriate table.
 */
class DoubleValueDisplay : DefaultFormatter() {
  private var si: Iterator<Map.Entry<Key, Value>>? = null

  override fun initialize(scanner: Iterable<Map.Entry<Key, Value>>, config: FormatterConfig) {
//    checkState(false)
    si = scanner.iterator()
//    this.config = FormatterConfig(config)
    super.initialize(scanner, config)
  }

  override fun next(): String {
    checkState(true)

    val orig = si!!.next()
    val nv = Value(dlex.decode(orig.value.get()).toString().toByteArray())
    val next = AbstractMap.SimpleImmutableEntry(orig.key, nv)

    return formatEntry(next)
  }
}


