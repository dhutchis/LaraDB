package edu.washington.cs.laragraphulo.examples.ktruss

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.security.Authorizations
import org.junit.Test
import org.junit.jupiter.api.Assertions.*

class KTruss_Lara_Accumulo_Example : AccumuloTestBase() {
  @Test
  fun test() {
    /* Obtain AccumuloConfig (connection information for Accumulo instance).
    In your code, you would write something similar to:
      val ac = AccumuloConfigImpl(instanceName = "instance", zookeeperHosts = "localhost:2181",
        username = "root", authenticationToken = PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with. */
    val ac = tester.accumuloConfig

    // Ingest ktruss graph data into Accumulo
    ac.ingestData(KTrussQuery.table0, KTrussQuery.initialSchema.defaultPSchema(), KTrussQuery.exampleData, true)

    // Delete the result table if it exists
    GraphuloUtil.deleteTables(ac.connector, KTrussQuery.table2)

    val resultTable = KTrussQuery.table2
    KTrussQuery.kTrussAdj(ac, KTrussQuery.table0, KTrussQuery.table2)

    // Print the covariances by scanning the result table from the query.
    printVerifyTable(resultTable, KTrussQuery.initialSchema.defaultPSchema())
  }


  fun printVerifyTable(table: String, ps: PSchema) {
    println("Table $table")
    tester.accumuloConfig.connector.tableOperations().clearLocatorCache(table)
    tester.accumuloConfig.connector.createScanner(table, Authorizations.EMPTY).use { scanner ->
      val expIter = KTrussQuery.expectedData.iterator()
      scanner.iterator().forEach { (k,v) ->
        // The TupleByKeyValue class decodes the Accumulo key-values using the physical schmea they are encoded in.
        val t = TupleByKeyValue(ps, k, v)
        print("Got: $t   ")
        assertTrue(expIter.hasNext())
        val e = expIter.next()
        println("Expect: $e")
        KTrussQuery.initialSchema.keys.forEach {
          val n = it.name
          assertTrue(n in t)
          assertTrue(n in e)
          assertEquals(e[n], t[n])
        }
      }
      assertFalse(expIter.hasNext())
    }
  }


}