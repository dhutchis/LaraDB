package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import com.sun.research.ws.wadl.Param
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.Column
import org.junit.Assert.*
import java.util.*

/**
 *
 */
class MergerTest(
    val params: Params
) {

  data class Params(
      /** name of the test */
      val name: String,
      /**
       *
       */
      val inputs: List<Iterable<Tuple>>,
      val prefixSize: Int,
//      val baps: List<BagAccessPath>,
      val emitNoMatches: Set<Int> = setOf(),
      val expected: Iterable<Tuple>
  ) {
    override fun toString(): String = name
  }



  class AssertingMergeCollider(val prefixSize: Int): Collider {
    override fun schema(inputs: List<BagAccessPath>): BagAccessPath {
      if (inputs.isEmpty()) {
        return BagAccessPath.build(setOf(), setOf(), setOf(), 0, false)
      }
      val input0 = inputs[0]
      assertTrue("Prefix $prefixSize exceeds key attributes ${input0.keyAttributes}", prefixSize <= input0.keyAttributes.size)
      ensureSamePrefix(inputs, prefixSize)
      ensureSortedUpto(inputs, prefixSize)

      // dap = the common prefix attributes
      // lap = the rest, in order of the iterators

      val dapCopySize = Math.min(prefixSize, input0.dap.size)
      val dap = ImmutableList.builder<Name>().addAll(input0.dap.subList(0, dapCopySize))
      val prefixAfterDap = prefixSize - dapCopySize

      val lapCopySize = Math.min(prefixAfterDap, input0.lap.size)
      dap.addAll(input0.lap.subList(0, lapCopySize))
      assertEquals(prefixSize, dapCopySize + lapCopySize)

      val lap = ImmutableList.builder<Name>()
      for (input in inputs) {
        lap.addAll(input.dap.subList(dapCopySize, input.dap.size))
        lap.addAll(input.lap.subList(lapCopySize, input.lap.size))
      }

      val capmap = HashMap<Name,ImmutableList.Builder<Name>>()
      for (input in inputs) {
        for (cf in input.cap) {
          if (cf.name in capmap)
            capmap[cf.name]!!.addAll(cf.attributes)
          else
            capmap[cf.name] = ImmutableList.builder<Name>().addAll(cf.attributes)
        }
      }
      val cap = ImmutableList.builder<ColumnFamily>()
      capmap.forEach { n, builder -> cap.add(ColumnFamily(n, builder.build())) }

      val bdap = dap.build(); val blap = lap.build(); val bcap = cap.build()
      val sortedUpto = bdap.size+blap.size // the iterator will maintain sorted order
      // ************* special cases with each iterator's sortedUpto --- ONEROWA vs ONEROWB vs always safe TWOROW
      return BagAccessPath.build(
         bdap, blap, bcap, sortedUpto, false
      )
    }

    private val ALL_ACTIVE = BooleanArray(prefixSize, {true})

    override fun collide(inputs: List<Iterator<Tuple>>, actives: BooleanArray): Iterator<Tuple> {
      assertArrayEquals("expect all iterators active for join", ALL_ACTIVE, actives)

      // stream each iterator into a list until the iterators advance past their current prefix
      // need the comparator from Merger to tell when that occurs
      // then, do the Cartesian product in sorted order, in the order of the iterators, constructing new tuples from the product
      // take inspiration from Graphulo
      // what does Union do differently? Element-wise union - no cartesian products
      // TODO
    }
  }


  companion object {
    fun tuple(vararg vals: String): Tuple =
        MutableByteTuple(vals.map { ArrayByteSequence(it.toByteArray()) }.toMutableList())

    val ti1 = listOf(tuple("1a", "2a"))
    val ti2 = listOf(tuple("1b", "2b"))
    val ti3 = listOf(tuple("1a", "2c"))

    val t1 = Params(
        name = "one tuple each; no match",
        inputs = listOf(ti1, ti2),
        prefixSize = 1,
        expected = listOf()
    )
    val t2 = Params(
        name = "one tuple each; match",
        inputs = listOf(ti1, ti3),
        prefixSize = 1,
        expected = listOf(tuple("1a", "2a", "2c"))
    )
    val t3 = Params(
        name = "one tuple each; cartesian product",
        inputs = listOf(ti1, ti2),
        prefixSize = 0,
        expected = listOf(tuple("1a", "2a", "1b", "2b"))
    )
  }

}
