package edu.washington.cs.laragraphulo.examples

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.sensor.*
import edu.washington.cs.laragraphulo.sensor.SensorCalc.SensorOpt
import edu.washington.cs.laragraphulo.util.DebugUtil
import org.apache.accumulo.core.client.Connector
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger
import java.io.StringWriter
import java.net.URL
import java.util.*

/**
 * Example demonstrating
 * (1) how to parse sensor data from two CSV files and ingest into Accumulo,
 * (2) how to run a complex sensor query that computes covariances, and
 * (3) how to scan the result.
 *
 * Run this example via `mvn test -Dtest=SensorExample`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 *
 * This data came from Array of Things sensors placed in Seattle and Denver.
 * See <https://arrayofthings.github.io/>.
 */
class SensorExample : AccumuloTestBase() {

  /** The main entry point of this example. START READING HERE. */
  @Test
  fun sensorExample() {
    /*  SETUP CONNECTOR
    In your code, you would write something similar to:
      val cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000)
      val instance = ZooKeeperInstance(cc)
      val conn = instance.getConnector("root", new PasswordToken("secret"))
    In this example, we use the connector that the test environment is built with.
     */
    val conn = tester.accumuloConfig.connector

    logger.debug{"=== Parse and ingest sensor data ==="}
    ingestSensors(conn)

    // The following object contains code to execute the sensor query.
    // It requires a credential (e.g. password) for the Accumulo instance.
    val credential = tester.accumuloConfig.authenticationToken
    val sensorQuery = SensorCalc(conn, credential, tablenameA, tablenameB, opts)

    // Execute the query
    val t = sensorQuery.timeAll()
    logger.debug{t}

    // Display the result covariances
    // (You can put the names of other tables you wish to view in here, e.g. intermediary tables
    logger.debug{"Now viewing results of the query, covariance:"}
    listOf(sensorQuery.sensorC).forEach {
      val sw = StringWriter()
      DebugUtil.printTable(it, conn, it, 14, sw::write) {
        // This decodes the result, from a byte-array back to a String, if the Encode optimization is enabled
        it.get().toDouble(SensorCalc.SensorOpt.Encode in opts).toString()
      }
      logger.debug(sw.toString())
    }

    // In case you scan the result table from the console, this Formatter displays the covariances in human-readable form
    if (SensorOpt.Encode in opts)
      conn.tableOperations().setProperty(sensorQuery.sensorC, "table.formatter", "edu.washington.cs.laragraphulo.sensor.DoubleValueDisplay")
  }


  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<SensorExample>()

    /**
     * This is the set of optimizations that are enabled for the sensor query in this example.
     * You can remove some optimizations and observe their effect on the query runtime.
     */
    private val opts: Set<SensorOpt> = {
      val s = EnumSet.noneOf(SensorOpt::class.java)
      s.add(SensorOpt.Encode)
      s.add(SensorOpt.FilterPush)
      s.add(SensorOpt.MonotoneSortElim)
      s.add(SensorOpt.ZeroDiscard)
      s.add(SensorOpt.AggregatePush)
      s.add(SensorOpt.PropagatePartition)
      s.add(SensorOpt.SymmetricCovariance)
      s.add(SensorOpt.Defer)
      s.add(SensorOpt.ReuseSource)
      s
    }()

    // You can change the following files and table names, e.g. to larger data sets.
    /** Data for the first sensor */
    const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"
    /** Data for the second sensor */
    const val filepathB = "data/sensor/bee-denver-v2dec-2017-02-06-tiny.txt"
    /** Name of the table to ingest the first sensor's data into */
    const val tablenameA = "ex_bee_uw_20170206"
    /** Name of the table to ingest the second sensor's data into */
    const val tablenameB = "ex_bee_denver_20170206"


    /** Ingest two sensors' data into tables */
    private fun ingestSensors(conn: Connector) {
      val encodeFunction = if (SensorOpt.Encode in opts) EncodeFunction.LEX else EncodeFunction.STRING
      listOf(
          filepathA to tablenameA,
          filepathB to tablenameB
      ).forEach { (filepath, tablename) ->
        val (cnt, t) = time { ingestSensor(conn, filepath, tablename, encodeFunction) }
        logger.debug{"Created $tablename and wrote $cnt entries to it from $filepath in $t seconds"}
      }
    }

    /** Ingest a sensor's data into a table
     * @return The number of entries written to Accumulo. */
    fun ingestSensor(conn: Connector, filepath: String, tablename: String, encodeFunction: EncodeFunction): Long {
      // Find the file that contains the sensor data and check that it exists.
      val url: URL = Thread.currentThread().contextClassLoader.getResource(filepath)
      Assert.assertNotNull(url)
      val file = url

      // Create an action to parse the CSV file and ingest it into Accumulo.
      // See SensorFileAction for details.
      val action = SensorFileAction.ingestAction(conn, tablename,
          // (if the Encode optimization is enabled, then user a byte-array encoding rather than a String encoding)
          encodeFunction,
          // (if the table to ingest already exists, delete and recreate it)
          recreateTable = true)
      // If you wish to simply see the entries from the CSV files rather than insert them into Accumulo,
      // replace the above with: SensorFileAction.printAction(System.out)

      // Execute the action.
      return action(file)
    }
  }
}
