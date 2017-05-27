package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import edu.washington.cs.laragraphulo.sensor.SensorFileAction
import org.junit.Test
import org.junit.jupiter.api.Assertions.*
import java.util.*

/** Data for the first sensor */
const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"



class APITest  { // : AccumuloTestBase()


  @Test
  fun test() {
    val data1: MutableList<Tuple> = LinkedList()

    val tcvListFileAction = SensorFileAction.SensorFileActionImpl<MutableList<Tuple>>(
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
    val tableMap = mapOf("sensorA" to data1, "sensorB" to data1)

    val C1 = SensorQuery.C.instantiateLoad(tableMap)
//    println(C1)

    val tupleIterator = C1.run()
    val list: MutableList<Tuple> = LinkedList()
    tupleIterator.forEach { list += it }
    assertTrue(list.isNotEmpty())

    tupleIterator.seek(TupleSeekKey())
    assertTrue(Iterators.elementsEqual(tupleIterator, list.iterator()))

    val last = list.last()
    tupleIterator.seek(TupleSeekKey(MyRange.greaterThan(last)))
    assertTrue(!tupleIterator.hasNext())

    tupleIterator.seek(TupleSeekKey(MyRange.atLeast(last)))
    assertTrue(tupleIterator.hasNext())
    assertTrue(tupleIterator.next() == last)
    assertTrue(!tupleIterator.hasNext())

    println(SensorQuery.C)
    val Cpipelines: List<TupleOp> = SensorQuery.C.splitPipeline()
    println(Cpipelines.joinToString("\n","[\n","\n]"))
  }



}