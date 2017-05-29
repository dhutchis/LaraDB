package edu.washington.cs.laragraphulo.examples.sensor

import edu.washington.cs.laragraphulo.api.MyRange
import edu.washington.cs.laragraphulo.api.Tuple
import edu.washington.cs.laragraphulo.api.instantiateLoad
import org.junit.jupiter.api.Assertions.*

/**
 * This example runs the task in [SensorQuery] as specified in the Lara API.
 * It executes as a standalone program (no Accumulo).
 *
 * This example
 * (1) parses sensor data from two CSV files and reads them into memory,
 * (2) executes a [SensorQuery] that computes covariances, all running on data in memory,
 *     and displays the result.
 *
 * Run this example via `mvn test -Dtest=Sensor_Lara_Standalone_Example`.
 * The test uses a MiniAccumuloInstance by default; setup the file GraphuloTest.conf to use a real Accumulo instance.
 *
 * This data came from Array of Things sensors placed in Seattle and Denver.
 * See <https://arrayofthings.github.io/>.
 */
class Sensor_Lara_Standalone_Example { // : AccumuloTestBase()
  @org.junit.Test
  fun test() {
    // Read sensor data into memory
    val sensorData = mapOf(
        "sensorA" to readSensorData(edu.washington.cs.laragraphulo.examples.sensor.Sensor_Graphulo_Example.Companion.filepathA),
        "sensorB" to readSensorData(edu.washington.cs.laragraphulo.examples.sensor.Sensor_Graphulo_Example.Companion.filepathB)
    )

    // Instantiate the sensor query with the data
    val C1 = SensorQuery.C.instantiateLoad(sensorData)
    println(C1)

    // Construct an iterator that implements the query. Read the results into memory.
    val tupleIterator = C1.run()
    val list: MutableList<edu.washington.cs.laragraphulo.api.Tuple> = java.util.LinkedList()
    tupleIterator.forEach { list += it }
    assertTrue(list.isNotEmpty())


    // The remaining code ensures that the sensor query runs even if we only want some range of the results.
    tupleIterator.seek(edu.washington.cs.laragraphulo.api.TupleSeekKey())
    assertTrue(com.google.common.collect.Iterators.elementsEqual(tupleIterator, list.iterator()))

    val last = list.last()
    tupleIterator.seek(edu.washington.cs.laragraphulo.api.TupleSeekKey(MyRange.greaterThan(last)))
    assertTrue(!tupleIterator.hasNext())

    tupleIterator.seek(edu.washington.cs.laragraphulo.api.TupleSeekKey(MyRange.atLeast(last)))
    assertTrue(tupleIterator.hasNext())
    assertTrue(tupleIterator.next() == last)
    assertTrue(!tupleIterator.hasNext())
  }

  fun readSensorData(filepath: String): List<edu.washington.cs.laragraphulo.api.Tuple> {
    val sensorData: MutableList<edu.washington.cs.laragraphulo.api.Tuple> = java.util.LinkedList()

    val tcvListFileAction = edu.washington.cs.laragraphulo.sensor.SensorFileAction.SensorFileActionImpl<MutableList<Tuple>>(
        { sensorData },
        { l, t, c, v ->
          l += mapOf("t" to t, "c" to c, "v" to v); l
        },
        { }
    )
    val urlA = Thread.currentThread().contextClassLoader.getResource(filepath)
    tcvListFileAction(urlA)
    sensorData.sortWith(edu.washington.cs.laragraphulo.api.KeyComparator(SensorQuery.initialSchema.keys))
    return sensorData
  }
}