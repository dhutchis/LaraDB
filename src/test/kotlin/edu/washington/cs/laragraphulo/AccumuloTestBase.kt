package edu.washington.cs.laragraphulo

import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.TestName
import org.slf4j.Logger

/**
 * Similar to [org.apache.accumulo.harness.AccumuloIT].
 */
abstract class AccumuloTestBase {

  @JvmField
  @Rule
  var testName = TestName()

  fun getUniqueNames(num: Int): Array<String> = Array(num) { i ->
    "test_${this.javaClass.simpleName}_${testName.methodName}$i"
  }

  companion object : Loggable {
    override val logger: Logger = logger<AccumuloTestBase>()

    @JvmField
    @ClassRule val tester = TEST_CONFIG.accumuloTester
  }
}
