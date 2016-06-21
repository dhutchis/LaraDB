package edu.washington.cs

import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.iteratortest.IteratorTestCaseFinder
import org.apache.accumulo.iteratortest.IteratorTestInput
import org.apache.accumulo.iteratortest.IteratorTestOutput
import org.apache.accumulo.iteratortest.junit4.BaseJUnit4IteratorTest
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase
import org.junit.runners.Parameterized
import java.nio.ByteBuffer


class IteratorHarnessTest(input: IteratorTestInput,
                          output: IteratorTestOutput,
                          testCase: IteratorTestCase)
      : BaseJUnit4IteratorTest(input, output, testCase) {

  // static stuff
  companion object {
    fun intToBytes(i: Int): ByteArray =
        ByteBuffer.allocate(4).putInt(i).array()

    val DATA = sortedMapOf(
          Key("r","cf","cq") to Value(intToBytes(1)),
          Key("r2","cf","cq") to Value(intToBytes(2))
      )


    @Suppress("unused")
    @JvmStatic
    @Parameterized.Parameters
    fun parameters(): Array<Array<Any>> {
      val input = IteratorTestInput(VersioningIterator::class.java, mapOf(), Range(), DATA)
      val output = IteratorTestOutput(DATA)
      val tests = IteratorTestCaseFinder.findAllTestCases()
      return createParameters(input, output, tests)
    }
  }


}