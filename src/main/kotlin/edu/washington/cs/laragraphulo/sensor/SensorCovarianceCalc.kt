package edu.washington.cs.laragraphulo.sensor

import com.google.common.collect.Iterators
import edu.mit.ll.graphulo.DynamicIteratorSetting
import edu.mit.ll.graphulo.Graphulo
import edu.mit.ll.graphulo.apply.ApplyIterator
import edu.mit.ll.graphulo.apply.ApplyOp
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply
import edu.mit.ll.graphulo.reducer.ReducerSerializable
import edu.mit.ll.graphulo.rowmult.MultiplyOp
import edu.mit.ll.graphulo.simplemult.MathTwoScalar
import edu.mit.ll.graphulo.skvi.DebugInfoIterator
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
import org.apache.hadoop.io.Text
import java.io.IOException
import java.util.*

class SensorCovarianceCalc(
    val conn: Connector,
    val pw: PasswordToken,
    val sensorA: String,
    val sensorB: String,
    val doString: Boolean = true
) {
  val sensorX = "${sensorA}_${sensorB}_X"
  val sensorU = "${sensorA}_${sensorB}_U"
  val sensorC = "${sensorA}_${sensorB}_C"

  val G by lazy { Graphulo(conn, pw) }

//  enum class Sensor { A, B }

  private fun recreate(vararg tns: String) {
    tns.forEach { tn ->
      if (conn.tableOperations().exists(tn))
        conn.tableOperations().delete(tn)
      conn.tableOperations().create(tn)
    }
  }

  private val AverageValuesByRow = DynamicIteratorSetting(21, "avgByRow")
      .append(AppendCounterApply.iteratorSetting(1, doString))
      .append(CombineSumCnt.iteratorSetting(1, doString))
      .append(DividePairApply.iteratorSetting(1, doString))
      .toIteratorSetting()
  private val subtract = MathTwoScalar::class.java // TODO: Breaks under true numeric encoding. Works for string encoding.
  private val subtractOptions = MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.MINUS, MathTwoScalar.ScalarType.DOUBLE, null, true)

  fun binAndDiff(): Long {
    require(conn.tableOperations().exists(sensorA)) {"table $sensorA does not exist"}
    require(conn.tableOperations().exists(sensorB)) {"table $sensorB does not exist"}
    recreate(sensorX)

    val itersBefore = DynamicIteratorSetting(21, "bin")
        .append(BinRowApply.iteratorSetting(1, doString))
        .append(AverageValuesByRow)
        .iteratorSettingList
    val tCounter = RowCountReduce()
    tCounter.init(emptyMap<String,String>().toMutableMap(), null)

    G.TwoTableEWISE(sensorA, sensorB, null, sensorX, // transpose to [c,t']
        -1, subtract, subtractOptions,
        null, null, null, null, false, false,
        itersBefore, itersBefore, null, tCounter, null, -1, null, null)
    val tCount = tCounter.serializableForClient
    println("tCount is $tCount")
    return tCount
  }


  fun meanAndSubtract() {
    require(conn.tableOperations().exists(sensorX)) {"table $sensorX does not exist"}
    recreate(sensorU)

    val leftIters = DynamicIteratorSetting(21, "avgLeftByRow")
        .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))
        .append(AverageValuesByRow)
        .iteratorSettingList
//    , IteratorSetting(1, DebugInfoIterator::class.java))

    G.TwoTableROWCartesian(TwoTableIterator.CLONESOURCE_TABLENAME, sensorX, null, sensorU, // transpose to [t',c]
        -1, MinusRowEwiseRight::class.java, MinusRowEwiseRight.optionMap(doString),
        null, null, null, null, false, false, false, false, false, false,
        leftIters, null, null, null, null, -1, null, null)

  }

  /**
   * Divide by tCount in the end.
   */
  fun covariance(tCount: Long) {
    require(tCount > 0) {"Bad tCount: $tCount"}
    require(conn.tableOperations().exists(sensorU)) {"table $sensorU does not exist"}
    recreate(sensorC)

    G.TableMult(TwoTableIterator.CLONESOURCE_TABLENAME, sensorU, sensorC, null,
        -1, MathTwoScalar::class.java, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE, null, false), // drop zero
        Graphulo.PLUS_ITERATOR_DOUBLE, // discards zeros
        null, null, null, false, false,
        null, null, listOf(IteratorSetting(1,DebugInfoIterator::class.java)),
        null, null, -1, null, null)

    GraphuloUtil.applyIteratorSoft(
        GraphuloUtil.addOnScopeOption(
            MathTwoScalar.applyOpDouble(Graphulo.DEFAULT_COMBINER_PRIORITY+1, true, MathTwoScalar.ScalarOp.DIVIDE, tCount.toDouble(), false),
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
  var t_string: Boolean = true

  val binSize = 60000

  companion object {
    val T_STRING = "T_STRING"
    private val ULL = ULongLexicoder()

    fun iteratorSetting(priority: Int, t_string: Boolean): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, BinRowApply::class.java.name)
      if (!t_string)
        itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + T_STRING, t_string.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    if (options.containsKey(T_STRING))
      t_string = options[T_STRING]!!.toBoolean()
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    val t = k.rowData.toArray().let { if (t_string) String(it).toLong() else ULL.decode(it) }
    val tm = t % binSize
    val tb = t - tm + (if (tm >= binSize/2) binSize else 0) // new t
    val rb = if (t_string) tb.toString().toByteArray() else ULL.encode(tb) // new row
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
  var t_string: Boolean = true

  companion object {
    val T_STRING = "T_STRING"
    private val dlex = DoubleLexicoder()
    private val lex = PairLexicoder(DoubleLexicoder(), ULongLexicoder())
    private val commaOne = ",1".toByteArray()

    fun iteratorSetting(priority: Int, t_string: Boolean): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, AppendCounterApply::class.java.name)
      if (!t_string)
        itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + T_STRING, t_string.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    if (options.containsKey(T_STRING))
      t_string = options[T_STRING]!!.toBoolean()
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    if (t_string) {
      val varr = v.get()
      val sz = varr.size
      val nvarr = Arrays.copyOf(varr, sz+commaOne.size)
      System.arraycopy(commaOne, 0, nvarr, sz, commaOne.size)
      return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(k,Value(nvarr)))
    } else {
      val nv = lex.encode(ComparablePair(dlex.decode(v.get()), 1L))
      return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry(k,Value(nv)))
    }
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
  var t_string: Boolean = true

  override fun reduce(key: Key, iter: Iterator<Value>): Value {
    var sum = 0.0
    var count: Long = 0L

    if (!iter.hasNext()) return Value()
    var next: Value? = iter.next()
    if (!iter.hasNext()) return next!!

    do {
      if (t_string) {
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

    val ret = if (t_string) "$sum,$count".toByteArray() else lex.encode(ComparablePair(sum, count))
    return Value(ret)
  }

  @Throws(IOException::class)
  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment?) {
    super.init(source, options, env)
    if (options.containsKey(T_STRING))
      t_string = options[T_STRING]!!.toBoolean()
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    val io = super.describeOptions()
    io.setName("CombineSumCnt")
    io.setDescription("Combiner that keeps track of sum and count of pairs of doubles and longs")
    io.addNamedOption(T_STRING, "True for String encoding, false for pair lexicoding (default true)")
    return io
  }

  companion object {
    val T_STRING = "T_STRING"
    private val lex = PairLexicoder(DoubleLexicoder(), ULongLexicoder())

    fun iteratorSetting(priority: Int, t_string: Boolean = true,
                        columns: List<IteratorSetting.Column> = emptyList()): IteratorSetting {
      val itset = IteratorSetting(priority, CombineSumCnt::class.java)
      if (columns.isEmpty())
        Combiner.setCombineAllColumns(itset, true)
      else
        Combiner.setColumns(itset, columns)
      if (!t_string)
        itset.addOption(T_STRING, t_string.toString())
      return itset
    }
  }

}


/**
 * Divides value interpreted as a pair, `first / second`.
 */
class DividePairApply : ApplyOp {
  /** Whether to use string encoding or to use numeric encoding. */
  var t_string: Boolean = true

  companion object {
    val T_STRING = "T_STRING"
    private val dlex = DoubleLexicoder()
    private val lex = PairLexicoder(DoubleLexicoder(), ULongLexicoder())

    fun iteratorSetting(priority: Int, t_string: Boolean): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, DividePairApply::class.java.name)
      if (!t_string)
        itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + T_STRING, t_string.toString())
      return itset
    }
  }

  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    if (options.containsKey(T_STRING))
      t_string = options[T_STRING]!!.toBoolean()
  }

  override fun seekApplyOp(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
  }

  override fun apply(k: Key, v: Value): MutableIterator<MutableMap.MutableEntry<Key, Value>> {
    if (t_string) {
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
  var t_string: Boolean = true
  var keep_zero: Boolean = true

  companion object {
    val T_STRING = "T_STRING"
    val KEEP_ZERO = "KEEP_ZERO"
    private val dlex = DoubleLexicoder()

    fun optionMap(t_string: Boolean, keep_zero: Boolean = true): Map<String, String> {
      val map = HashMap<String, String>()
      if (!t_string) map.put(T_STRING, t_string.toString())
      if (!keep_zero) map.put(KEEP_ZERO, keep_zero.toString())
      return map
    }
  }


  override fun init(options: MutableMap<String, String>, env: IteratorEnvironment?) {
    if (options.containsKey(T_STRING)) t_string = options[T_STRING]!!.toBoolean()
    if (options.containsKey(KEEP_ZERO)) keep_zero = options[KEEP_ZERO]!!.toBoolean()
  }

  override fun multiply(Mrow: ByteSequence,
                        ATcolF: ByteSequence, ATcolQ: ByteSequence, ATcolVis: ByteSequence, ATtime: Long,
                        BcolF: ByteSequence, BcolQ: ByteSequence, BcolVis: ByteSequence, Btime: Long,
                        ATval: Value, Bval: Value): Iterator<Map.Entry<Key, Value>> {
    if (!keep_zero && ATval == Bval) return Collections.emptyIterator()
    val a = if (t_string) ATval.toString().toDouble() else dlex.decode(ATval.get())
    val b = if (t_string) Bval.toString().toDouble() else dlex.decode(Bval.get())
    val r = a - b
    if (!keep_zero && r == 0.0) return Collections.emptyIterator()
    val nv = Value(if (t_string) r.toString().toByteArray() else dlex.encode(r))
    val k = Key(Mrow.toArray(), ATcolF.toArray(), BcolQ.toArray())
    return Iterators.singletonIterator(AbstractMap.SimpleImmutableEntry<Key, Value>(k, nv))
  }
}



