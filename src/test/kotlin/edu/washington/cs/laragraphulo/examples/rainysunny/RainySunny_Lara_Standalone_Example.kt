package edu.washington.cs.laragraphulo.examples.rainysunny

import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.junit.jupiter.api.Assertions.assertEquals


/**
 * Example demonstrating a simple Lara Ext function call using the Lara API.
 * The query and data are given in [RainySunnyQuery].
 *
 * Run this example via `mvn test -Dtest=RainySunny_Lara_Standalone_Example`.
 * The test does not use Accumulo.
 */
class RainySunny_Lara_Standalone_Example {
  @org.junit.Test
  fun rainySunnyExample() {
    // Instantiate the query with actual data, so that we can run it here.
    val query = RainySunnyQuery.query.instantiateLoad(mapOf(RainySunnyQuery.table1 to RainySunnyQuery.rainyData))

    // ============= RUN! (and print the result)
    val scannedData = query.run().toList()
    scannedData.forEach { logger.debug {it} }

    // ============= CHECK CORRECTNESS
    assertEquals(scannedData, RainySunnyQuery.sunnyData)
  }

  
  /** Static variables and methods */
  companion object : edu.washington.cs.laragraphulo.Loggable {
    /** This is used for logging messages */
    override val logger: org.slf4j.Logger = logger<RainySunny_Lara_Standalone_Example>()

    fun <T> Iterator<T>.toList(): List<T> {
      val l = java.util.LinkedList<T>()
      while (this.hasNext())
        l += this.next()
      return l
    }
  }
}