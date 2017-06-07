package edu.washington.cs.laragraphulo.examples.tricount

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.security.Authorizations
import org.junit.Test
import org.junit.jupiter.api.Assertions.*

class TriCount_Lara_Accumulo : AccumuloTestBase() {
  companion object {
    const val table0 = "testTriCount0"
  }


  @Test
  fun test() {
    /* Obtain AccumuloConfig (connection information for Accumulo instance).
    In your code, you would write something similar to:
      val ac = AccumuloConfigImpl(instanceName = "instance", zookeeperHosts = "localhost:2181",
        username = "root", authenticationToken = PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with. */
    val ac = tester.accumuloConfig

    // Ingest ktruss graph data into Accumulo
    ac.ingestData(table0, TriCountQuery.initialSchema.defaultPSchema(), TriCountQuery.exampleData, true)

    val triCount = TriCountQuery.triCount(ac, table0)
    println("triCount is $triCount")
    assertEquals(TriCountQuery.expectedTriCount, triCount)
  }

}