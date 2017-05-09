package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import org.junit.Assert.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.slf4j.Logger
import java.util.*

class TupleEncodeTest {

  companion object : Loggable {
    override val logger: Logger = logger<TupleEncodeTest>()

    @JvmStatic @Suppress("UNUSED")
    fun testCases() = TupleEncodeFieldTest.testCases()

    private fun signum(i: Int) = if (i < 0) -1 else if (i == 0) 0 else 1

    private val rand = Random().apply { this.nextLong().let { logger.debug{"random seed: $it"}; this.setSeed(it) } }
    /** Allocates the given attributes randomly around the various parts of a PhysicalSchema.
     * Does not use column visibility and does not put STRING data into the timestamp. */
    fun genRandomSchema(ins0: List<PAttribute<*>>): PhysicalSchema {
      val useTs = ins0.any { it.type != PType.STRING } && rand.nextBoolean()
//      val useVis = rand.nextBoolean()
      val (ts,ins) = if (useTs) ins0.first { it.type != PType.STRING }.let { tstype -> tstype to ins0.filter { it != tstype } } else null to ins0
      val num = ins.size
      val par = DoubleArray(4) { -Math.log(rand.nextDouble()) }.let { arr -> arr.sum().let { sum ->
        val arrInt = arr.map { (it / sum * ins.size + .5).toInt() }.toMutableList()
        while (arrInt.sum() != ins.size) {
          val maxVal = arrInt.max()
          arrInt[arrInt.withIndex().filter { it.value == maxVal }.let { it[rand.nextInt(it.size)].index }] += signum(ins.size - arrInt.sum())
        }
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
          pvals = ins.subList(pre[3],pre[4]).map { it.asPValAttribute() }
      ).apply { logger.debug{"schema: $this"} }
    }
  }

  @ParameterizedTest
  @MethodSource(names = arrayOf("testCases"))
  fun testEncodeRandom(pair: Pair<List<PAttribute<*>>, List<NameTuple>>) {
    val (attrs,tuples) = pair
    val ps = genRandomSchema(attrs)
    val iter = KvToTupleAdapter(ps, TupleToKvAdapter(ps, tuples.iterator()))
//    val iter = TupleOp.LoadData(ps, tuples)
    assertTrue(Iterators.elementsEqual(tuples.iterator(), iter))
  }
}