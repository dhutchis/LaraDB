package edu.washington.cs.laragraphulo.opt

import org.junit.Assert.*

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
      val baps: List<BagAccessPath>,
      val expected: Iterable<Tuple>
  ) {
    override fun toString(): String = name
  }
}
