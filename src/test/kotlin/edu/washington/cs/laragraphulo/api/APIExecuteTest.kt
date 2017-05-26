package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.sensor.SensorFileAction
import org.junit.Test
import org.junit.jupiter.api.Assertions.*
import java.util.*

///** Data for the first sensor */
//const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"



class APIExecuteTest //: AccumuloTestBase()
{


  @Test
  fun test() {
    val query = TupleOp.Store(SensorQuery.C, "sensorC")
    val Cpipelines: List<TupleOp.Store> = query.splitPipeline()
    println(Cpipelines.joinToString("\n","[\n","\n]"))
  }



}