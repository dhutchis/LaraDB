package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import org.junit.Assert.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.opt.ABS
import org.slf4j.Logger
import java.util.*

class TupleEncodeTest {

  companion object : Loggable {
    override val logger: Logger = logger<TupleEncodeTest>()

    val pschema0 = listOf<PAttribute<*>>()
    val tuples0 = listOf(mapOf<String,Any?>())
    val pschema1 = listOf(PAttribute("id",PType.INT),PAttribute("t",PType.LONG))
    val tuples1 = listOf(mapOf("id" to 0, "t" to 1L),mapOf("id" to 1, "t" to Long.MAX_VALUE),mapOf("id" to -1, "t" to 0L))
    val pschema2 = listOf(PAttribute("t",PType.LONG),PAttribute("c",PType.STRING))
    val tuples2 = listOf(mapOf("t" to 1L, "c" to ""),mapOf("t" to Long.MAX_VALUE, "c" to "high"),mapOf("t" to 0L, "c" to "ZERO"))
    val pschema2r = pschema2.reversed()
    val pschema3 = listOf(PAttribute("c",PType.STRING),PAttribute("t",PType.LONG_VARIABLE))
    val tuples3 = listOf(mapOf("t" to 1L, "c" to ""),mapOf("t" to Long.MAX_VALUE, "c" to "high"),mapOf("t" to 0L, "c" to "ZERO"))
    val pschema4 = listOf(PAttribute("t",PType.LONG),PAttribute("c",PType.STRING), PAttribute("v", PType.DOUBLE))
    val tuples4 = listOf(mapOf("t" to 1L, "c" to "", "v" to 0.0),mapOf("t" to Long.MAX_VALUE, "c" to "high", "v" to 1.0),mapOf("t" to 0L, "c" to "ZERO", "v" to -3.5))
    @JvmStatic @Suppress("UNUSED")
    fun testCases() = TupleEncodeFieldTest.testCases()
    @JvmStatic @Suppress("UNUSED")
    fun extraDataCases() = listOf(pschema2 to tuples4, pschema2r to tuples4, pschema3 to tuples4)

    private val rand = Random()
    fun genRandomSchema(ins0: List<PAttribute<*>>): PhysicalSchema {
      val useTs = ins0.any { it.type != PType.STRING } && rand.nextBoolean()
//      val useVis = rand.nextBoolean()
      val (ts,ins) = if (useTs) ins0.first { it.type != PType.STRING }.let { tstype -> tstype to ins0.filter { it != tstype } } else null to ins0
      val num = ins.size
      val par = DoubleArray(4) { -Math.log(rand.nextDouble()) }.let { arr -> arr.sum().let { sum ->
        val arrInt = arr.map { (it / sum * ins.size).toInt() }.toMutableList()
        while (arrInt.sum() != ins.size)
          arrInt[arrInt.indexOf(arrInt.max())] += ins.size - arrInt.sum()
        arrInt.toList()
      } }
      assert(par.sum() == num) {"partition $par out of ${ins.count()} items (ts is $ts)"}
      val pre = par.fold(listOf(0)) { list, p -> list + (list.last() + p) } // 2, 1, 0, 2 --> 0, 2, 3, 3, 5
      return PhysicalSchema(
          row = ins.subList(pre[0], pre[1]),
          family = ins.subList(pre[1], pre[2]),
          colq = ins.subList(pre[2], pre[3]),
          vis = null,
          ts = ts,
          vals = ins.subList(pre[3],pre[4])
      ).apply { logger.debug{"schema: $this"} }
    }
  }

  @ParameterizedTest
  @MethodSource(names = arrayOf("testCases"))
  fun testEncodeRandom(pair: Pair<List<PAttribute<*>>, List<NameTuple>>) {
    val (attrs,tuples) = pair
    val ps = genRandomSchema(attrs)
    val iter = KvToTupleAdapter(ps, TupleToKvAdapter(ps, tuples.iterator()))
//    val iter = NameTupleOp.ScanFromData(ps, tuples.iterator())
    assertTrue(Iterators.elementsEqual(tuples.iterator(), iter))
  }
}