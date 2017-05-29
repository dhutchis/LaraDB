package edu.washington.cs.laragraphulo.examples.wordcount

import edu.mit.ll.graphulo.util.GraphuloUtil
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.api.TupleOp
import edu.washington.cs.laragraphulo.api.execute
import edu.washington.cs.laragraphulo.api.ingestData
import edu.washington.cs.laragraphulo.api.scanAccumulo
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.junit.Test
import org.junit.jupiter.api.Assertions
import org.slf4j.Logger
import java.util.*

/**
 * Example demonstrating a simple Lara Ext function call using the Lara API.
 * The query and data are given in [WordCountQuery].
 *
 * Run this example via `mvn test -Dtest=WordCount_Lara_Accumulo_Example`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 */
class WordCount_Lara_Accumulo_Example : AccumuloTestBase() {

  @Test
  fun wordCountExample() {
    /* Obtain AccumuloConfig (connection information for Accumulo instance).
    In your code, you would write something similar to:
      val ac = AccumuloConfigImpl(instanceName = "instance", zookeeperHosts = "localhost:2181",
        username = "root", authenticationToken = PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with. */
    val ac = tester.accumuloConfig

    // ============= INGEST INPUT DATA
    val initialSchemaPhysical = WordCountQuery.initialSchema.defaultPSchema()
    ac.ingestData(WordCountQuery.table1, initialSchemaPhysical, WordCountQuery.exampleData, deleteIfExists = true)

    // Delete the result table if it exists
    GraphuloUtil.deleteTables(ac.connector, table2)

    // ============= RUN QUERY ON ACCUMULO
    val query = TupleOp.Store(WordCountQuery.query, table2)
    ac.execute(query)

    // ============= SCAN RESULT FROM ACCUMULO
    val scannedData = ac.scanAccumulo(table2).toList()
    logger.debug{scannedData}

    // ============= CHECK CORRECTNESS
    Assertions.assertEquals(scannedData, WordCountQuery.expectedData)
  }


  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<WordCount_Lara_Accumulo_Example>()

    fun <T> Iterator<T>.toList(): List<T> {
      val l = LinkedList<T>()
      while (this.hasNext())
        l += this.next()
      return l
    }

    const val table2 = "testWordCount2"
  }
}