package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableListMultimap
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.ArrayByteSequence
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*
import edu.washington.cs.laragraphulo.util.TestUtil.checkSorted

/** First int is the iterator number, second int is the attribute number within that iterator.
 * This is for the attributes after the common prefix. */
typealias TupleRef = Pair<Int,Int>


/**
 *
 */
@RunWith(Parameterized::class)
class MergerTest(
    val params: Params
) {

  data class Params(
      /** name of the test */
      val name: String,
      /**
       *
       */
      val inputs: List< Pair<List<Tuple>, SortedAccessPath> >,
      val prefixSize: Int,
//      val baps: List<ImmutableBagAccessPath>,
      val emitNoMatches: Set<Int> = setOf(),
      val expected: List<Tuple>
  ) {
    override fun toString(): String = name
  }



  class AssertingMergeCollider(val prefixSize: Int): Collider {

    lateinit var tupleReferences: List<TupleRef>
    lateinit var ALL_ACTIVE: BooleanArray
    var lastTuples: List<Tuple>? = null

    override fun <T> schema(inputs: List<T>): SortedAccessPath where T : SortedKeySchema, T : APKeySchema {
//    override fun schema(inputs: List<SortedAccessPath>): SortedAccessPath {
      if (inputs.isEmpty())
        return SortedAccessPath.of(listOf(), listOf(), sortedUpto = 0)
      val input0 = inputs[0]
      assertTrue("Prefix $prefixSize exceeds key attributes ${input0.keyNames}", prefixSize <= input0.keyNames.size)
      ensureSamePrefix(inputs, prefixSize)
      ensureSortedUpto(inputs, prefixSize)
      ALL_ACTIVE = BooleanArray(inputs.size, {true})
      val tupleRefs = ImmutableList.builder<TupleRef>()

      // dapNames = the common prefix attributes
      // lapNames = the rest, in order of the iterators

      val dapCopySize = Math.min(prefixSize, input0.dapNames.size)
      val dap = ImmutableList.builder<Name>().addAll(input0.dapNames.subList(0, dapCopySize))
      val prefixAfterDap = prefixSize - dapCopySize

      val lapCopySize = Math.min(prefixAfterDap, input0.lapNames.size)
      dap.addAll(input0.lapNames.subList(0, lapCopySize))
      assertEquals(prefixSize, dapCopySize + lapCopySize)

      val lap = ImmutableList.builder<Name>()
      for ((index, input) in inputs.withIndex()) {
        lap.addAll(input.dapNames.subList(dapCopySize, input.dapNames.size))
        for (p in dapCopySize..input.dapNames.size-1)
          tupleRefs.add(index to p)
        lap.addAll(input.lapNames.subList(lapCopySize, input.lapNames.size))
        for (p in input.dapNames.size+lapCopySize..input.dapNames.size+input.lapNames.size-1)
          tupleRefs.add(index to p)
      }

//      val capmap = HashMap<Name, Pair<ImmutableList.Builder<Name>,ImmutableList.Builder<TupleRef>>>()
//      for ((index, input) in inputs.withIndex()) {
//        var p = input.dapNames.size+input.lapNames.size
//        for (cf in input.cap) {
//          val content = capmap[cf.name] ?: Pair(ImmutableList.builder<Name>(), ImmutableList.builder<TupleRef>())
//          content.first.addAll(cf.attributes)
//          content.second.addAll((p..cf.attributes.size-1).map { index to it })
//          capmap[cf.name] = content
//          p += cf.attributes.size
//        }
//      }
//      val cap = ImmutableList.builder<ColumnFamily>()
//      capmap.forEach { n, pair -> cap.add(ColumnFamily(n, pair.first.build())); tupleRefs.addAll(pair.second.build()) }

      tupleReferences = tupleRefs.build()
      println("tupleReferences is $tupleReferences")
      val bdap = dap.build(); val blap = lap.build()
      val sortedUpto = bdap.size+blap.size // the iterator will maintain sorted order
      // ************* special cases with each iterator's sortedUpto --- ONEROWA vs ONEROWB vs always safe TWOROW
      return SortedAccessPath.of(
         bdap, blap, sortedUpto = sortedUpto
      )
    }

    val rowComparator = TupleComparatorByKeyPrefix(prefixSize)

    val assertingMergeMultiplyOp: MultiplyOp = { tuples ->
      for (i in tuples.indices)
        assertEquals("tuples are not equal by rowComparator $rowComparator: ${Arrays.toString(tuples)}", 0, rowComparator.compare(tuples[0], tuples[i]))
      val list = ArrayList<ArrayByteSequence>(tupleReferences.size+prefixSize)
      list.addAll(tuples[0].keys.subList(0,prefixSize)) // the dapNames
      for ((tupleRef, attrRef) in tupleReferences) {
        list += tuples[tupleRef].keys[attrRef]
      }
      // todo - change behavior of family and value attributes
      Iterators.singletonIterator(TupleImpl(list, tuples[0].family, ImmutableListMultimap.of()))
    }

    override fun collide(inputs: List<PeekingIterator<Tuple>>, actives: BooleanArray): Iterator<Tuple> {
      assertArrayEquals("expect all iterators active for join", ALL_ACTIVE, actives)
      // check that values given in sorted order
      if (lastTuples != null)
        inputs.zip(lastTuples!!).forEach {
          assertTrue("iterators out of order; last value was ${it.second}; this value is ${it.first.peek()}",
              TupleComparatorByKeys.compare(it.first.peek(), it.second) >= 0)
        }
      lastTuples = inputs.map { it.peek() }


      // what does Union do differently? Element-wise union - no cartesian products

      // advances all active inputs to after this row, as required by the Collider contract
      //.filterIndexed { i, iter -> actives[i] }
      val inputTuplesAfterFirst = inputs.subList(1,inputs.size).map { readRow(rowComparator, it) }
      return CartesianIterator(OneRowIterator(rowComparator, inputs[0]),
          inputTuplesAfterFirst, assertingMergeMultiplyOp)
    }
  }

  @Test
  fun test() {
    // check that the inputs are sorted properly
    params.inputs.forEach {
      it.first.checkSorted(TupleComparatorByKeyPrefix(it.second.sortedUpto))
    }

    val collider = AssertingMergeCollider(params.prefixSize)
    val aps = params.inputs.map { it.second }
    val outAp = collider.schema(aps)
    println("outAp is $outAp")
    val actual = Merger(
        params.inputs.map { it.first.iterator() },
        params.prefixSize,
        collider,
        setOf()
    )
    assertIteratorsEqual(params.expected.iterator(), actual)
  }


  companion object {
//    private val log = LogManager.logger(MergerTest::class.java)

    fun <T> assertIteratorsEqual(i1: Iterator<T>, i2: Iterator<T>) {
      while (i1.hasNext()) {
        assertTrue("it2 $i2 ran out before it1 $i1", i2.hasNext())
        val o1 = i1.next()
        val o2 = i2.next()
        assertEquals("iterator elements not equal", o1, o2)
      }
      assertFalse("it1 $i1 ran out before it2 $i2", i2.hasNext())
    }

    fun tuple(vararg vals: String): Tuple =
        TupleImpl(vals.map { ArrayByteSequence(it.toByteArray()) }, EMPTY, ImmutableListMultimap.of())

    // todo - the code above does not do anything with string names in the schema
    val ti1 = listOf(tuple("1a", "2a"))
    val ti2 = listOf(tuple("1b", "2b"))
    val ti3 = listOf(tuple("1a", "2c"))
    val tiEmpty = listOf<Tuple>()
    val ti12 = listOf(tuple("1a","2b"), tuple("1a","2c"))
    val ti13 = listOf(tuple("1a","2d"), tuple("1a","2e"), tuple("1a","2f"))

    val data: Array<Params> = arrayOf(
        Params(
            name = "one tuple each; no match",
            inputs = listOf(ti1 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                ti2 to SortedAccessPath.of(listOf("a1"), listOf("b2"))),
            prefixSize = 1,
            expected = listOf()
        ),
        Params(
            name = "one tuple each; match",
            inputs = listOf(ti1 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                ti3 to SortedAccessPath.of(listOf("a1"), listOf("b2"))),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2a", "2c"))
        ),
        Params(
            name = "one tuple each; cartesian product",
            inputs = listOf(ti1 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                ti2 to SortedAccessPath.of(listOf("b1"), listOf("b2"))),
            prefixSize = 0,
            expected = listOf(tuple("1a", "2a", "1b", "2b"))
        ),
        Params(
            name = "one iter empty; match",
            inputs = listOf(ti1 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                tiEmpty to SortedAccessPath.of(listOf("a1"), listOf("b2"))),
            prefixSize = 1,
            expected = listOf()
        ),
        Params(
            name = "one iter empty; cartesian product",
            inputs = listOf(ti1 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                tiEmpty to SortedAccessPath.of(listOf("b1"), listOf("b2"))),
            prefixSize = 0,
            expected = listOf()
        ),
        Params(
            name = "1x2 match",
            inputs = listOf(ti1 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                ti12 to SortedAccessPath.of(listOf("a1"), listOf("b2"))),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2a", "2b"), tuple("1a","2a","2c"))
        ),
        Params(
            name = "2x3 match",
            inputs = listOf(ti12 to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                ti13 to SortedAccessPath.of(listOf("a1"), listOf("b2"))),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2b", "2d"), tuple("1a","2b","2e"), tuple("1a","2b","2f"),
                tuple("1a", "2c", "2d"), tuple("1a","2c","2e"), tuple("1a","2c","2f"))
        ),
        Params(
            name = "2x1 + 1x2 match",
            inputs = listOf(
                listOf(tuple("1a","2b"), tuple("1a","2c"), tuple("1b","2x")) to SortedAccessPath.of(listOf("a1"), listOf("a2")),
                listOf(tuple("1a","2g"), tuple("1b","2y"), tuple("1b","2z")) to SortedAccessPath.of(listOf("a1"), listOf("b2"))),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2b", "2g"), tuple("1a","2c","2g"),
                tuple("1b", "2x", "2y"), tuple("1b","2x","2z"))
        )
    )

    @JvmStatic
    @Parameterized.Parameters(name = "test {index}: {0}")
    fun parameters(): Array<out Any> = data
  }

}
