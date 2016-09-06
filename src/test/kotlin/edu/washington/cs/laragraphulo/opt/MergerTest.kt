package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import org.apache.accumulo.core.data.ArrayByteSequence
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*

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
      val inputs: List< Pair<Iterable<Tuple>,BagAccessPath> >,
      val prefixSize: Int,
//      val baps: List<BagAccessPath>,
      val emitNoMatches: Set<Int> = setOf(),
      val expected: Iterable<Tuple>
  ) {
    override fun toString(): String = name
  }



  class AssertingMergeCollider(val prefixSize: Int): Collider {

    /** First int is the iterator number, second int is the attribute number within that iterator.
     * This is for the attributes after the common prefix. */
    typealias TupleRef = Pair<Int,Int>

    lateinit var tupleReferences: List<TupleRef>
    lateinit var ALL_ACTIVE: BooleanArray
    var lastTuples: List<Tuple>? = null

    override fun schema(inputs: List<BagAccessPath>): BagAccessPath {
      if (inputs.isEmpty())
        return BagAccessPath.build(setOf(), setOf(), setOf(), 0, false)
      val input0 = inputs[0]
      assertTrue("Prefix $prefixSize exceeds key attributes ${input0.keyNames}", prefixSize <= input0.keyNames.size)
      ensureSamePrefix(inputs, prefixSize)
      ensureSortedUpto(inputs, prefixSize)
      ALL_ACTIVE = BooleanArray(inputs.size, {true})
      val tupleRefs = ImmutableList.builder<TupleRef>()

      // dap = the common prefix attributes
      // lap = the rest, in order of the iterators

      val dapCopySize = Math.min(prefixSize, input0.dap.size)
      val dap = ImmutableList.builder<Name>().addAll(input0.dap.subList(0, dapCopySize))
      val prefixAfterDap = prefixSize - dapCopySize

      val lapCopySize = Math.min(prefixAfterDap, input0.lap.size)
      dap.addAll(input0.lap.subList(0, lapCopySize))
      assertEquals(prefixSize, dapCopySize + lapCopySize)

      val lap = ImmutableList.builder<Name>()
      for ((index, input) in inputs.withIndex()) {
        lap.addAll(input.dap.subList(dapCopySize, input.dap.size))
        for (p in dapCopySize..input.dap.size-1)
          tupleRefs.add(index to p)
        lap.addAll(input.lap.subList(lapCopySize, input.lap.size))
        for (p in input.dap.size+lapCopySize..input.dap.size+input.lap.size-1)
          tupleRefs.add(index to p)
      }

      val capmap = HashMap<Name, Pair<ImmutableList.Builder<Name>,ImmutableList.Builder<TupleRef>>>()
      for ((index, input) in inputs.withIndex()) {
        var p = input.dap.size+input.lap.size
        for (cf in input.cap) {
          val content = capmap[cf.name] ?: Pair(ImmutableList.builder<Name>(), ImmutableList.builder<TupleRef>())
          content.first.addAll(cf.attributes)
          content.second.addAll((p..cf.attributes.size-1).map { index to it })
          capmap[cf.name] = content
          p += cf.attributes.size
        }
      }
      val cap = ImmutableList.builder<ColumnFamily>()
      capmap.forEach { n, pair -> cap.add(ColumnFamily(n, pair.first.build())); tupleRefs.addAll(pair.second.build()) }

      tupleReferences = tupleRefs.build()
      println("tupleReferences is $tupleReferences")
      val bdap = dap.build(); val blap = lap.build(); val bcap = cap.build()
      val sortedUpto = bdap.size+blap.size // the iterator will maintain sorted order
      // ************* special cases with each iterator's sortedUpto --- ONEROWA vs ONEROWB vs always safe TWOROW
      return BagAccessPath.build(
         bdap, blap, bcap, sortedUpto, false
      )
    }

    val rowComparator = TupleComparatorByPrefix(prefixSize)

    val assertingMergeMultiplyOp: MultiplyOp = { tuples ->
      for (i in tuples.indices)
        assertEquals("tuples are not equal by rowComparator $rowComparator: ${Arrays.toString(tuples)}", 0, rowComparator.compare(tuples[0], tuples[i]))
      val list = ArrayList<ArrayByteSequence>(tupleReferences.size+prefixSize)
      list.addAll(tuples[0].subList(0,prefixSize)) // the dap
      for ((tupleRef, attrRef) in tupleReferences) {
        list += tuples[tupleRef][attrRef]
      }
      Iterators.singletonIterator(ByteTuple(list))
    }

    override fun collide(inputs: List<PeekingIterator<Tuple>>, actives: BooleanArray): Iterator<Tuple> {
      assertArrayEquals("expect all iterators active for join", ALL_ACTIVE, actives)
      // check that values given in sorted order
      if (lastTuples != null)
        inputs.zip(lastTuples!!).forEach {
          assertTrue("iterators out of order; last value was ${it.second}; this value is ${it.first.peek()}",
            it.first.peek().compareTo(it.second) >= 0)
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
      val sortedList = ArrayList(it.first.toList())
      Collections.sort(sortedList, TupleComparatorByPrefix(it.second.sortedUpto))
      assertIteratorsEqual(sortedList.iterator(), it.first.iterator())
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
//    private val log = LogManager.getLogger(MergerTest::class.java)

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
        ByteTuple(vals.map { ArrayByteSequence(it.toByteArray()) })

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
            inputs = listOf(ti1 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                ti2 to BagAccessPath.build(listOf("a1"), listOf("b2"), listOf())),
            prefixSize = 1,
            expected = listOf()
        ),
        Params(
            name = "one tuple each; match",
            inputs = listOf(ti1 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                ti3 to BagAccessPath.build(listOf("a1"), listOf("b2"), listOf())),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2a", "2c"))
        ),
        Params(
            name = "one tuple each; cartesian product",
            inputs = listOf(ti1 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                ti2 to BagAccessPath.build(listOf("b1"), listOf("b2"), listOf())),
            prefixSize = 0,
            expected = listOf(tuple("1a", "2a", "1b", "2b"))
        ),
        Params(
            name = "one iter empty; match",
            inputs = listOf(ti1 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                tiEmpty to BagAccessPath.build(listOf("a1"), listOf("b2"), listOf())),
            prefixSize = 1,
            expected = listOf()
        ),
        Params(
            name = "one iter empty; cartesian product",
            inputs = listOf(ti1 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                tiEmpty to BagAccessPath.build(listOf("b1"), listOf("b2"), listOf())),
            prefixSize = 0,
            expected = listOf()
        ),
        Params(
            name = "1x2 match",
            inputs = listOf(ti1 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                ti12 to BagAccessPath.build(listOf("a1"), listOf("b2"), listOf())),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2a", "2b"), tuple("1a","2a","2c"))
        ),
        Params(
            name = "2x3 match",
            inputs = listOf(ti12 to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                ti13 to BagAccessPath.build(listOf("a1"), listOf("b2"), listOf())),
            prefixSize = 1,
            expected = listOf(tuple("1a", "2b", "2d"), tuple("1a","2b","2e"), tuple("1a","2b","2f"),
                tuple("1a", "2c", "2d"), tuple("1a","2c","2e"), tuple("1a","2c","2f"))
        ),
        Params(
            name = "2x1 + 1x2 match",
            inputs = listOf(
                listOf(tuple("1a","2b"), tuple("1a","2c"), tuple("1b","2x")) to BagAccessPath.build(listOf("a1"), listOf("a2"), listOf()),
                listOf(tuple("1a","2g"), tuple("1b","2y"), tuple("1b","2z")) to BagAccessPath.build(listOf("a1"), listOf("b2"), listOf())),
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
