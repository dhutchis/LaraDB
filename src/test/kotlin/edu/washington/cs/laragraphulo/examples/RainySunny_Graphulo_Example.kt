package edu.washington.cs.laragraphulo.examples

import edu.mit.ll.graphulo.Graphulo
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.security.Authorizations
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger


/**
 * This example demonstrates how to use the Graphulo library to execute a query with a single custom iterator.
 * The example first ingests the data in [exampleData] into Accumulo [table1], which includes the string "Hello Rainy World!"
 * The custom iterator changes all instances of the word "Rainy" to "Sunny".
 * The Graphulo query runs this query on [table1] in Accumulo and stores the result in [table2].
 * Finally, the example scans [table2] from Accumulo and prints out its entries, verifying that they are Sunny as expected.
 *
 * Run this example via `mvn test -Dtest=RainySunny_Graphulo_Example`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 */
class RainySunny_Graphulo_Example : AccumuloTestBase() {

  /** The main entry point of this example. START READING HERE. */
  @Test
  fun rainySunnyExample() {
    val table1 = "exHelloWorld1"
    val table2 = "exHelloWorld2"

    /*  SETUP CONNECTOR
    In your code, you would write something similar to:
      val cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000)
      val instance = ZooKeeperInstance(cc)
      val conn = instance.getConnector("root", new PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with.
     */
    val conn = tester.accumuloConfig.connector

    // Delete the tables if they exist, so that we can start with fresh tables
    setOf(table1,table2).forEach { table ->
      if (conn.tableOperations().exists(table)) {
        logger.debug{"$table already exists; deleting to start with a fresh table"}
        conn.tableOperations().delete(table)
      }
      conn.tableOperations().create(table)
    }
    
    logger.debug{"Now ingesting example data into $table1"}
    ingestHelloRainyWorld(conn, table1)
    
    // Create a Graphulo object to do query operations.
    // This requires a credential (e.g. password) for the Accumulo instance.
    val G = Graphulo(conn, tester.accumuloConfig.authenticationToken)
    
    // Perform an operation which copies data from one table into another table
    // with a MAP that modifies the tuples.
    logger.debug{"Now running the query with a map iterator from $table1 to $table2"}
    runQuery(G, table1, table2)

    logger.debug{"Now viewing results of the query in $table2"}
    viewAndVerifyResult(conn, table2)
  }
  


  
  
  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<RainySunny_Graphulo_Example>()

    private val exampleData = listOf(
        /* Row to Value */
        "msg1" to "Hello World!",
        "msg2" to "Hello Glorious World!",
        "msg3" to "Hello Rainy World!"
    )

    private val expectedResult = exampleData.map {(k,v) -> k to v.replace("Rainy", "Sunny")}
    
    /** Ingest example data into table */
    private fun ingestHelloRainyWorld(conn: Connector, table: String) {
      conn.createBatchWriter(table, BatchWriterConfig()).use { bw ->
        exampleData.forEach { (k, v) ->
          val m = Mutation(k)
          m.put("", "", v)
          bw.addMutation(m)
        }
      }
    }

    /**
     * Perform an operation which copies data from one table into another table
     * with a map iterator that modifies the tuples.
     * 
     * @param G A Graphulo object on which to perform the query
     */
    private fun runQuery(G: Graphulo, table1: String, table2: String) {
      // Create an IteratorSetting for the map iterator.
      val iteratorList = listOf(MapRainToSunIterator.iteratorSetting(1))
      
      // Execute the query. Most of the parameters to the general Graphulo OneTable function are null or -1, 
      // which means "use default" or "don't use this extra feature".
      val numEntries = G.OneTable(table1, table2, null, null, -1, null, null, null, null, null,
          iteratorList, null, null)
      logger.debug {"The query generated $numEntries entries that were written into $table2"}
    }


    /** View the contents of the second table and verify that it produced the expected result. */
    private fun viewAndVerifyResult(conn: Connector, table: String) {
      conn.createScanner(table, Authorizations.EMPTY).use { scanner ->
        val scannedData = scanner.map { (k,v) -> k.row.toString() to v.toString() }
        scannedData.forEach { logger.debug {it} }
        Assert.assertEquals(scannedData, expectedResult)
      }
    }

  }
}