package edu.washington.cs.laragraphulo.iter

import com.google.common.collect.Iterators
import edu.washington.cs.laragraphulo.iter.MergeJoinTest.Companion.parameters
import edu.washington.cs.laragraphulo.util.MapToSkviAdapter
import edu.washington.cs.laragraphulo.util.SeekData
import edu.washington.cs.laragraphulo.util.TestUtil
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*
import kotlin.test.assertTrue

/**
 * MergeJoinTest is initialized with input data as a parameter.
 * The expected output is from a mock Collider.
 *
 * This class is a [Parameterized] test.
 * For each parameter in the [parameters],
 * JUnit constructs a [MergeJoinTest] passing in that parameter as a constructor argument
 * and runs all the tests defined inside.
 *
 * @param[input] Each item defines the entries and initial seek of a [SortedKeyValueIterator],
 *        along with what ShipKeyComparator is used.
 */
@RunWith(Parameterized::class)
class MergeJoinTest(
    val params: Params
) {

  data class Params(
      /** name of the test */
      val name: String,
      val input: List<Pair<SortedMap<Key, Value>, SeekData>>,
      val comparator: MergeJoin.SkipKeyComparator,
      val expected: List<List<SortedMap<Key, Value>>>
  ) {
    override fun toString(): String {
      return name
    }
  }

  private fun SortedMap<Key,Value>.toSkvi(): SortedKeyValueIterator<Key,Value> {
    val skvi = MapToSkviAdapter(this)
    skvi.init(null, mapOf(), TestUtil.MockIteratorEnvironment(TestUtil.MyIteratorScope.SCAN))
    return skvi
  }

  private fun SortedKeyValueIterator<Key,Value>.seek(seekData: SeekData): SortedKeyValueIterator<Key,Value> {
    this.seek(seekData.range, seekData.columnFamilies, seekData.inclusive)
    return this
  }

  /**
   * Multiplies the SortedSets of all matching entries.
   */
  abstract class AllCollider(
      val comparator: Comparator<Key>
  ) : Collider
  {
    override fun collide(vararg skvis: SortedKeyValueIterator<Key, Value>): Iterator<Pair<Key, Value>> {
      val sets = Array<SortedMap<Key,Value>>(skvis.size, { TreeMap() })
      for (i in skvis.indices) {
        val skvi = skvis[i]
        assertTrue(skvi.hasTop())
        val mapRes: SortedMap<Key, Value> = sets[i]
        mapRes.put(skvi.topKey, skvi.topValue)
        val origKey = skvi.topKey
        skvi.next() // assume transitivity: if a == b && b == c, then a == c
        while (skvi.hasTop() && comparator.compare(origKey, skvi.topKey) == 0) {
          mapRes.put(skvi.topKey, skvi.topValue)
          skvi.next()
        }
      }
      return collide(*sets)
    }

    abstract fun collide(vararg sets: SortedMap<Key,Value>): Iterator<Pair<Key,Value>>
  }

  /**
   * Use this collider to see which entries match during a run.
   */
  class PrintAllCollider(
      comparator: Comparator<Key>
  ) : AllCollider(comparator) {
    override fun collide(vararg sets: SortedMap<Key, Value>): Iterator<Pair<Key, Value>> {
      for (i in sets.indices) {
        println("match set $i: ${sets[i]}")
      }
      println()
      return Iterators.emptyIterator()
    }
  }

  class AssertingCollider(
      comparator: Comparator<Key>,
      val expected: List<List<SortedMap<Key, Value>>>
  ) : AllCollider(comparator)
  {
    private val iter = expected.iterator()
    private var collidecnt = 0

    override fun collide(vararg sets: SortedMap<Key, Value>): Iterator<Pair<Key, Value>> {
      collidecnt++
      val thisExp = iter.next()
      Assert.assertArrayEquals("problem on call $collidecnt to collide", thisExp.toTypedArray(), sets)
      return Iterators.emptyIterator()
    }
  }

  @Test
  fun test() {
    val arr: Array<Pair<SortedKeyValueIterator<Key, Value>, SeekData>> =
        params.input.map {
          Pair(it.first.toSkvi().seek(it.second), it.second)
        }.toTypedArray()

    val mj =
//        MergeJoin(params.comparator, PrintAllCollider(params.comparator), *arr)
        MergeJoin(params.comparator, AssertingCollider(params.comparator, params.expected), *arr)
    while (mj.hasNext())
      mj.next()
  }



  companion object {
//    fun intToBytes(i: Int): ByteArray =
//        ByteBuffer.allocate(4).putInt(i).array()

    val t1 = Params(
        name = "1-skvi row-wise all",
        input = listOf(
            sortedMapOf(
                Key("r", "", "1") to Value(),
                Key("r", "", "2") to Value(),
                Key("r2", "", "1") to Value()
            ) to SeekData.ALL
        ),
        comparator = MergeJoin.SkipKeyComparator.ROW, expected = listOf(
            listOf( // first call to collide
                sortedMapOf( // first skvi
                    Key("r", "", "1") to Value(),
                    Key("r", "", "2") to Value()
                )
            ),
            listOf( // second call to collide
                sortedMapOf(Key("r2", "", "1") to Value())
            )
        )
    )

    val tcolfamfilterInclusive = Params(
        name = "1-skvi row-wise colfam-inclusive-filter",
        input = listOf(
            sortedMapOf(
                Key("r", "a", "1") to Value(),
                Key("r", "b", "2") to Value(),
                Key("r2", "a", "1") to Value()
            ) to SeekData(Range(), setOf(ArrayByteSequence("a")), true)
        ),
        comparator = MergeJoin.SkipKeyComparator.ROW, expected = listOf(
            listOf(
                sortedMapOf(
                    Key("r", "a", "1") to Value()
                )
            ),
            listOf(
                sortedMapOf(Key("r2", "a", "1") to Value())
            )
        )
    )
    val tcolfamfilterExclusive = Params(
        name = "1-skvi row-wise colfam-exclusive-filter",
        input = listOf(
            sortedMapOf(
                Key("r", "a", "1") to Value(),
                Key("r", "b", "2") to Value(),
                Key("r2", "a", "1") to Value()
            ) to SeekData(Range(), setOf(ArrayByteSequence("a")), false)
        ),
        comparator = MergeJoin.SkipKeyComparator.ROW, expected = listOf(
        listOf(
            sortedMapOf(
                Key("r", "b", "2") to Value()
            )
        )
      )
    )

    val data: Array<Params> = arrayOf(
        t1,
        Params(
            name = "1-skvi row-wise seeked",
            input = listOf(
                sortedMapOf(
                    Key("r", "", "1") to Value(),
                    Key("r", "", "2") to Value(),
                    Key("r2", "", "1") to Value(),
                    Key("r2", "", "2") to Value(),
                    Key("r3", "", "2") to Value()
                ) to SeekData(Range("r2", true, null, false), setOf(), false)
            ),
            comparator = MergeJoin.SkipKeyComparator.ROW, expected = listOf(
                listOf(
                    sortedMapOf(
                        Key("r2", "", "1") to Value(),
                        Key("r2", "", "2") to Value()
                    )
                ),
                listOf(
                    sortedMapOf(Key("r3", "", "2") to Value())
                )
            )
        ),
        Params(
            name = "2-skvi row-wise",
            input = listOf(
                sortedMapOf(
                    Key("r", "", "1") to Value(),
                    Key("r", "", "2") to Value(),
                    Key("r3", "", "3") to Value()
                ) to SeekData.ALL,
                sortedMapOf(
                    Key("r2", "", "1") to Value(),
                    Key("r2", "", "2") to Value(),
                    Key("r3", "", "4") to Value()
                ) to SeekData.ALL
            ),
            comparator = MergeJoin.SkipKeyComparator.ROW, expected = listOf(
                listOf(
                    sortedMapOf(
                        Key("r3", "", "3") to Value()
                    ),
                    sortedMapOf(
                        Key("r3", "", "4") to Value()
                    )
                )
            )
        ),
        tcolfamfilterInclusive,
        tcolfamfilterExclusive,
        Params(
            name = "3-skvi row-wise",
            input = listOf(
                sortedMapOf(
                    Key("r", "", "1") to Value(),
                    Key("r", "", "2") to Value(),
                    Key("r3", "", "3") to Value(),
                    Key("r5", "", "7") to Value()
                ) to SeekData.ALL,
                sortedMapOf(
                    Key("r2", "", "1") to Value(),
                    Key("r2", "", "2") to Value(),
                    Key("r3", "", "4") to Value(),
                    Key("r5", "", "8") to Value()
                ) to SeekData.ALL,
                sortedMapOf(
                    Key("r3", "", "5") to Value(),
                    Key("r3", "", "6") to Value(),
                    Key("r4", "", "2") to Value(),
                    Key("r5", "", "9") to Value()
                ) to SeekData.ALL
            ),
            comparator = MergeJoin.SkipKeyComparator.ROW, expected = listOf(
            listOf(
                sortedMapOf(
                    Key("r3", "", "3") to Value()
                ),
                sortedMapOf(
                    Key("r3", "", "4") to Value()
                ),
                sortedMapOf(
                    Key("r3", "", "5") to Value(),
                    Key("r3", "", "6") to Value()
                )
            ),
            listOf(
                sortedMapOf(
                    Key("r5", "", "7") to Value()
                ),
                sortedMapOf(
                    Key("r5", "", "8") to Value()
                ),
                sortedMapOf(
                    Key("r5", "", "9") to Value()
                )
            )
        )
        )
    ) // end of test data

    @JvmStatic
    @Parameterized.Parameters(name = "test {index}: {0}")
    fun parameters(): Array<out Any> {
      return data
    }
  }



}