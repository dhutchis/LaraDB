package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.sensor.SensorFileAction
import edu.washington.cs.laragraphulo.api.TupleOp.*
import org.junit.Test
import java.util.*

/** Data for the first sensor */
const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"



class APITest  { // : AccumuloTestBase()





  @Test
  fun test() {
    val data1: MutableList<NameTuple> = LinkedList()

    val tcvListFileAction = SensorFileAction.SensorFileActionImpl<MutableList<NameTuple>>(
        { data1 },
        { l,t,c,v ->
          l += mapOf("t" to t, "c" to c, "v" to v)
          l
        },
        { }
    )
    val urlA = Thread.currentThread().contextClassLoader.getResource(filepathA)
    tcvListFileAction.invoke(urlA)
    data1.sortWith(KeyComparator(SensorQuery.initialSchema.keys))
    val tableMap = mapOf("sensorA" to data1.iterator(), "sensorB" to data1.iterator())

    val C = SensorQuery.C.instantiateLoad(tableMap)

    println()
    C.run().forEach { println(it) }
  }



}