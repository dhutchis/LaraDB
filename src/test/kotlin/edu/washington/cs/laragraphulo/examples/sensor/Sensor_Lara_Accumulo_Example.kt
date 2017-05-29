package edu.washington.cs.laragraphulo.examples.sensor

import edu.mit.ll.graphulo.util.GraphuloUtil
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.api.PSchema
import edu.washington.cs.laragraphulo.api.TupleByKeyValue
import edu.washington.cs.laragraphulo.api.TupleOp
import edu.washington.cs.laragraphulo.api.execute
import edu.washington.cs.laragraphulo.sensor.EncodeFunction
import org.apache.accumulo.core.security.Authorizations
import org.junit.Test

/**
 * This example runs the task in [SensorQuery] as specified in the Lara API.
 * It executes on an Accumulo instance.
 *
 * This example
 * (1) parses sensor data from two CSV files and ingests them into Accumulo,
 * (2) executes a [SensorQuery] that computes covariances on Accumulo and stores the result as an Accumulo table,
 * (3) scans and displays the result from Accumulo.
 *
 * Run this example via `mvn test -Dtest=Sensor_Lara_Accumulo_Example`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 *
 * This data came from Array of Things sensors placed in Seattle and Denver.
 * See <https://arrayofthings.github.io/>.
 */
class Sensor_Lara_Accumulo_Example : AccumuloTestBase() {
  @Test
  fun test() {
    /* Obtain AccumuloConfig (connection information for Accumulo instance).
    In your code, you would write something similar to:
      val ac = AccumuloConfigImpl(instanceName = "instance", zookeeperHosts = "localhost:2181",
        username = "root", authenticationToken = PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with.
     */
    val ac = tester.accumuloConfig

    // Ingest sensor data into Accumulo
    Sensor_Graphulo_Example.ingestSensor(ac.connector, Sensor_Graphulo_Example.filepathA, "sensorA", EncodeFunction.PTYPE)
    Sensor_Graphulo_Example.ingestSensor(ac.connector, Sensor_Graphulo_Example.filepathB, "sensorB", EncodeFunction.PTYPE)

    // Set Physical Schema of ingested sensor data to (row=t, colq=c, val=v)
    val aT = SensorQuery.attrT.defaultPhysical()  // Attributes t, c, v
    val aC = SensorQuery.attrC.defaultPhysical()
    val aV = SensorQuery.attrVn.defaultPhysical()
    val sensorSchema = PSchema(listOf(aT), colq = listOf(aC), pvals = listOf(aV))
    ac.setSchema("sensorA", sensorSchema)
    ac.setSchema("sensorB", sensorSchema)

    // Delete the result table if it exists
    GraphuloUtil.deleteTables(ac.connector, "sensorC")

    // This is the Sensor Query
    val query = TupleOp.Store(SensorQuery.C, "sensorC")

    // Execute it on Accumulo!
    ac.execute(query)

    // Print the covariances by scanning the result table from the query.
    printTable(query.table, query.resultSchema.defaultPSchema())
  }


  fun printTable(table: String, ps: PSchema) {
    tester.accumuloConfig.connector.createScanner(table, Authorizations.EMPTY).use { scanner ->
      scanner.iterator().forEach { (k,v) ->
        // The TupleByKeyValue class decodes the Accumulo key-values using the physical schmea they are encoded in.
        println(TupleByKeyValue(ps, k, v))
      }
    }
  }
}