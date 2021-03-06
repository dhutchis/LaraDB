package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableListMultimap
import com.google.common.collect.Iterators
import edu.washington.cs.laragraphulo.util.TestUtil.checkSorted
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*

/**
 * Converts KeyValues to Tuples and back, checking the conversion each way.
 */
@RunWith(Parameterized::class)
class KeyValueToTupleTest(
    val params: Params
) {

  data class Params(
      /** name of the test */
      val name: String,
      /**
       *
       */
      val inputs: List<KeyValue>,
      val apKeySchema: APKeySchema,
      val widthSchema: WidthSchema,
      val expected: List<Tuple>
  ) {
    override fun toString(): String = name
  }

  @Test
  fun test() {
    params.inputs.checkSorted(KeyValueComparatorByKey, "Input is not sorted")
    params.expected.checkSorted(TupleComparatorByKeys, "Expected Tuples are not sorted")

    val actual = ArrayList<Tuple>(params.inputs.size) // over-estimate
    for (tuple in KeyValueToTuple(Iterators.peekingIterator(params.inputs.iterator()), params.apKeySchema, params.widthSchema)) {
      actual.add(tuple)
    }
    Assert.assertEquals("The tuples converted from the KeyValues are different than expected", params.expected, actual)

    val recoveredKeyValues = actual.flatMap { it.toKeyValues(params.apKeySchema) }
    Assert.assertEquals("Could not recover original KeyValues after converting to tuples and back", params.inputs, recoveredKeyValues)
  }


  companion object {

    data class WidthSchemaImpl(override val widths: List<Width>): WidthSchema

    fun String.toABS() = this.toByteArray().let { ArrayByteSequence(it, 0, it.size) }

    private fun par(name: String, inputs: List<KeyValue>, ap: AccessPath, expected: List<Tuple>) =
        Params(name, inputs, ap, ap, expected)

    val data = arrayOf(
        par(
            name = "empty no-schema",
            inputs = listOf(),
            ap = AccessPath.of(listOf(), listOf(), widths = listOf()),
            expected = listOf()
        ),
        par(
            name = "1-k no-schema",
            inputs = listOf(Key("","fam","q") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf(), listOf(), widths = listOf()),
            expected = listOf(TupleImpl(
                listOf(),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE))))
        ),
        par(
            name = "1-k duplicate v no-schema",
            inputs = listOf(Key("","fam","q") to Value(), Key("","fam","q") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf(), listOf(), widths = listOf()),
            expected = listOf(
                TupleImpl(listOf(),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE),
                    "q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "1-k 2-v no-schema",
            inputs = listOf(Key("","fam","q") to Value(), Key("","fam","q2") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf(), listOf(), widths = listOf()),
            expected = listOf(
                TupleImpl(listOf(),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE),
                    "q2".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "2-k no-schema",
            inputs = listOf(Key("","fam","q") to Value(), Key("","fam2","q") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf(), listOf(), widths = listOf()),
            expected = listOf(
                TupleImpl(listOf(),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE))),
                TupleImpl(listOf(),"fam2".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "2-k 1-row-schema",
            inputs = listOf(Key("r","fam","q") to Value(), Key("r2","fam","q") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf("row"), listOf(), widths = listOf(-1)),
            expected = listOf(
                TupleImpl(listOf("r".toABS()),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE))),
                TupleImpl(listOf("r2".toABS()),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "2-k colq-schema",
            inputs = listOf(Key("","fam","abaaq") to Value(), Key("","fam","abbbq") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf(), listOf("cqPrePre","cqPre"), widths = listOf(3, 1)),
            expected = listOf(
                TupleImpl(listOf("aba".toABS(), "a".toABS()),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE))),
                TupleImpl(listOf("abb".toABS(), "b".toABS()),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "2-k all-schema",
            inputs = listOf(Key("r","fam","aq") to Value(), Key("r2","fam","bq") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf("row"), listOf("cqPre"), widths = listOf(-1, 1)),
            expected = listOf(
                TupleImpl(listOf("r".toABS(), "a".toABS()),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE))),
                TupleImpl(listOf("r2".toABS(), "b".toABS()),"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "big mess",
            inputs = listOf(Key("ar","fam","aaq") to Value("yes"), Key("ar","fam","aaq2") to Value("no"),
                Key("ar2","fam","abq33") to Value()).map { KeyValue(it) },
            ap = AccessPath.of(listOf("rowpre","row"), listOf("cqPre", "cqPre2"), widths = listOf(1, -1, 1, 1)),
            expected = listOf(
                TupleImpl(listOf("a","r","a","a").map { it.toABS() },"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue("yes".toABS(), EMPTY, Long.MAX_VALUE),
                    "q2".toABS(), FullValue("no".toABS(), EMPTY, Long.MAX_VALUE))),
                TupleImpl(listOf("a","r2","a","b").map { it.toABS() },"fam".toABS(),ImmutableListMultimap.of("q33".toABS(), FullValue(EMPTY, EMPTY, Long.MAX_VALUE)))
            )
        ),
        par(
            name = "big mess with visibility and timestamps",
            inputs = listOf(Key("ar","fam","aaq","xxx",42) to Value("yes"), Key("ar","fam","aaq2") to Value("no"),
                Key("ar2","fam","abq33", 33) to Value("bigo33")).map { KeyValue(it) },
            ap = AccessPath.of(listOf("rowpre","row"), listOf("cqPre", "cqPre2"), widths = listOf(1, -1, 1, 1)),
            expected = listOf(
                TupleImpl(listOf("a","r","a","a").map { it.toABS() },"fam".toABS(),ImmutableListMultimap.of("q".toABS(), FullValue("yes".toABS(), "xxx".toABS(), 42),
                    "q2".toABS(), FullValue("no".toABS(), EMPTY, Long.MAX_VALUE))),
                TupleImpl(listOf("a","r2","a","b").map { it.toABS() },"fam".toABS(),ImmutableListMultimap.of("q33".toABS(), FullValue("bigo33".toABS(), EMPTY, 33)))
            )
        )
    )

    @JvmStatic
    @Parameterized.Parameters(name = "test {index}: {0}")
    fun parameters(): Array<out Any> = data
  }


}