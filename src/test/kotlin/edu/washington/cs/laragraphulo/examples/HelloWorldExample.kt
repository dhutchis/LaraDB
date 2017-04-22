package edu.washington.cs.laragraphulo.examples

import edu.mit.ll.graphulo.Graphulo
import edu.mit.ll.graphulo.apply.ApplyIterator
import edu.mit.ll.graphulo.apply.ApplyOp
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.security.Authorizations
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger


/**
 * Example demonstrating
 * (1) how to ingest data into Accumulo,
 * (2) how to run a Graphulo query with a simple filter, and
 * (3) how to scan data from an Accumulo table.
 *
 * Run this example via `mvn test -Dtest=HelloWorldExample`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 */
class HelloWorldExample : AccumuloTestBase() {

  /** The main entry point of this example. START READING HERE. */
  @Test
  fun helloWorldExample() {
    val table1 = "exHelloWorld1"
    val table2 = "exHelloWorld2"

    /*  SETUP CONNECTOR
    In your code, you would write something similar to
      val cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000)
      val instance = ZooKeeperInstance(cc)
      val conn = instance.getConnector("root", new PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with.
     */
    val conn = tester.accumuloConfig.connector

    // Delete the tables if they exist, so that we can start with fresh tables
    setOf(table1,table2).forEach { table ->
      conn.tableOperations().let { 
        if (it.exists(table)) {
          logger.debug{"$table already exists; deleting to start with a fresh table"}
          it.delete(table)
        }
        it.create(table)
      }
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
  

  /** A simple iterator that changes [SUNNY] to [RAINY] in the Value of tuples seen. */
  class MapRainToSunIterator : ApplyOp {
    override fun init(options: Map<String, String>?, env: IteratorEnvironment?) {}
    override fun seekApplyOp(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {}
    
    override fun apply(k: Key, v: Value): Iterator<Map.Entry<Key, Value>> {
      val newValue = Value(v.toString().replace(RAINY, SUNNY))
      return mapOf(k to newValue).iterator()
    }

    companion object {
      /** A helper method that creates an [IteratorSetting], in order to use this in queries.  */
      fun iteratorSetting(priority: Int): IteratorSetting {
        val itset = IteratorSetting(priority, ApplyIterator::class.java)
        itset.addOption(ApplyIterator.APPLYOP, MapRainToSunIterator::class.java.name)
        return itset
      }
    }
  }
  
  
  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<HelloWorldExample>()

    const val RAINY = "Rainy"
    const val SUNNY = "Sunny"

    private val exampleData = listOf(
        /* Row to Value */
        "msg1" to "Hello World!",
        "msg2" to "Hello Glorious World!",
        "msg3" to "Hello Rainy World!"
    )
    
    
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
        val scannedData = scanner.toList()

        scannedData.zip(exampleData).forEach { (actual, expected) ->
          val (ak, av) = actual
          val (ek, ev) = expected
          logger.debug {"${ak.toStringNoTime()} -> $av"}
          Assert.assertEquals(ak.row.toString(), ek)
          Assert.assertEquals(av.toString(), ev.replace(RAINY, SUNNY))
        }

        Assert.assertEquals(scannedData.size, exampleData.size)
      }
    }

  }
}