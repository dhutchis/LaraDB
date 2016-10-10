package edu.washington.cs.laragraphulo.opt


import edu.washington.cs.laragraphulo.opt.*
import edu.washington.cs.laragraphulo.opt.raco.PTree
import org.apache.accumulo.core.data.ArrayByteSequence
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.StringReader
import java.nio.ByteBuffer
import edu.washington.cs.laragraphulo.opt.raco.RacoOperator
import edu.washington.cs.laragraphulo.opt.raco.RacoType
import edu.washington.cs.laragraphulo.opt.viz.generateDot
import org.junit.Assume
import java.util.concurrent.Callable

/**
 * Start with a Raco query, compile it, and execute the result.
 */
@RunWith(Parameterized::class)
class QueryOptimizeExecuteIT(
    val params: Params
) {


  data class Params (
      val name: String,
      val query: String,
      val beforeTasks: List<Callable<*>> = listOf(),
      val afterTasks: List<Callable<*>> = listOf()
  ) {
    override fun toString(): String = name
  }

}
