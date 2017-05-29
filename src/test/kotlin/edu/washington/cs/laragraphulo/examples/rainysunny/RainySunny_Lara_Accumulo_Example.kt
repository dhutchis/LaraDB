package edu.washington.cs.laragraphulo.examples.rainysunny

import edu.mit.ll.graphulo.util.GraphuloUtil
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.slf4j.Logger
import java.util.*


/**
 * Example demonstrating a simple Lara Ext function call using the Lara API.
 * The query and data are given in [RainySunnyQuery].
 *
 * Run this example via `mvn test -Dtest=RainySunny_Lara_Accumulo_Example`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 */
class RainySunny_Lara_Accumulo_Example : AccumuloTestBase() {
  @Test
  fun rainySunnyExample() {
    /* Obtain AccumuloConfig (connection information for Accumulo instance).
    In your code, you would write something similar to:
      val ac = AccumuloConfigImpl(instanceName = "instance", zookeeperHosts = "localhost:2181",
        username = "root", authenticationToken = PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with. */
    val ac = tester.accumuloConfig

    // ============= INGEST INPUT DATA
    val initialSchemaPhysical = RainySunnyQuery.initialSchema.defaultPSchema()
    ac.ingestData(RainySunnyQuery.table1, initialSchemaPhysical, RainySunnyQuery.rainyData, deleteIfExists = true)

    // Delete the result table if it exists
    GraphuloUtil.deleteTables(ac.connector, table2)

    // ============= RUN QUERY ON ACCUMULO
    val query = TupleOp.Store(RainySunnyQuery.query, table2)
    ac.execute(query)

    // ============= SCAN RESULT FROM ACCUMULO
    val scannedData = ac.scanAccumulo(table2).toList()
    scannedData.forEach { logger.debug {it} }

    // ============= CHECK CORRECTNESS
    assertEquals(scannedData, RainySunnyQuery.sunnyData)
  }

  
  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<RainySunny_Lara_Accumulo_Example>()

    fun <T> Iterator<T>.toList(): List<T> {
      val l = LinkedList<T>()
      while (this.hasNext())
        l += this.next()
      return l
    }

    const val table2 = "testRainySunny2"
  }
}