package edu.washington.cs.laragraphulo.examples.wordcount

import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.api.instantiateLoad
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.slf4j.Logger
import java.util.*


/**
 * Example demonstrating a simple Lara Ext and Union (Agg) function call using the Lara API.
 *
 * Run this example via `mvn test -Dtest=WordCount_Lara_Standalone_Example`.
 * The test does not use Accumulo.
 */
class WordCount_Lara_Standalone_Example {

  /** The main entry point of this example. START READING HERE. */
  @Test
  fun wordCountExample() {
    // Instantiate the query with actual data, so that we can run it here.
    val query = WordCountQuery.query.instantiateLoad(mapOf(WordCountQuery.table1 to WordCountQuery.exampleData))

    // ============= RUN! (and print the result)
    val scannedData = query.run().toList()
    logger.debug{scannedData}

    // ============= CHECK CORRECTNESS
    assertEquals(scannedData, expected)
  }
  

  
  
  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<WordCount_Lara_Standalone_Example>()

    fun <T> Iterator<T>.toList(): List<T> {
      val l = LinkedList<T>()
      while (this.hasNext())
        l += this.next()
      return l
    }
    
    private val expected = listOf(
        mapOf("count" to 1, "word" to "are"),
        mapOf("count" to 1, "word" to "by"),
        mapOf("count" to 1, "word" to "i'm"),
        mapOf("count" to 1, "word" to "if"),
        mapOf("count" to 1, "word" to "on"),
        mapOf("count" to 2, "word" to "sea"),
        mapOf("count" to 1, "word" to "seashells"),
        mapOf("count" to 2, "word" to "seashore"),
        mapOf("count" to 4, "word" to "sells"),
        mapOf("count" to 4, "word" to "she"),
        mapOf("count" to 4, "word" to "shells"),
        mapOf("count" to 1, "word" to "shore"),
        mapOf("count" to 1, "word" to "so"),
        mapOf("count" to 1, "word" to "sure"),
        mapOf("count" to 1, "word" to "surely"),
        mapOf("count" to 3, "word" to "the")
    )
  }
}