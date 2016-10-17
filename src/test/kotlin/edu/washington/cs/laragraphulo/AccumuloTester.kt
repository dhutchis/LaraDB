
package edu.washington.cs.laragraphulo

import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import org.junit.rules.TestRule

/**
 * Provides an Accumulo instance to a test method.
 * Handles setup and teardown.
 */
interface AccumuloTester : TestRule {


  val accumuloConfig: AccumuloConfig


}
