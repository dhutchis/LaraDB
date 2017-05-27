package edu.washington.cs.laragraphulo.api

import com.google.common.collect.Iterators
import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.examples.SensorExample
import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import edu.washington.cs.laragraphulo.opt.KeyValueToTuple
import edu.washington.cs.laragraphulo.opt.SerializationUtil
import edu.washington.cs.laragraphulo.sensor.EncodeFunction
import edu.washington.cs.laragraphulo.sensor.SensorFileAction
import edu.washington.cs.laragraphulo.util.TestUtil
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.NewTableConfiguration
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.junit.Test
import org.junit.jupiter.api.Assertions.*
import java.util.*

///** Data for the first sensor */
//const val filepathA = "data/sensor/bee-uw-v2dec-2017-02-06-tiny.txt"



class APIExecuteTest : AccumuloTestBase()
{


  @Test
  fun test() {
    val query = TupleOp.Store(SensorQuery.C, "sensorC")
    println(query)
    val Cpipelines: List<TupleOp.Store> = query.splitPipeline()
    println(Cpipelines.joinToString("\n","[\n","\n]"))

    val ac = tester.accumuloConfig

    // ingest sensor data
    // PSchema is (row=t, colq=c, val
    SensorExample.ingestSensor(ac.connector, SensorExample.filepathA, "sensorA", EncodeFunction.PTYPE)
    SensorExample.ingestSensor(ac.connector, SensorExample.filepathB, "sensorB", EncodeFunction.PTYPE)

    val aT = SensorQuery.attrT.defaultPhysical()
    val aC = SensorQuery.attrC.defaultPhysical()
    val aV = SensorQuery.attrVn.defaultPhysical()
    val sensorSchema = PSchema(listOf(aT), colq = listOf(aC), pvals = listOf(aV))

    listOf("sensorA","sensorB").forEach { table ->
      ac.setSchema(table, sensorSchema)
    }


    val pipelineFun: (TupleOp.Store) -> Unit = { store ->
      // create the table we want to store to and set its schema
//      val ntc = NewTableConfiguration().setProperties()
      print("Create ${store.table}. ")
      print("Schema ${store.resultSchema.defaultPSchema()}. ")
      ac.connector.tableOperations().create(store.table)
      ac.setSchema(store.table, store.resultSchema.defaultPSchema()) // matches that in TupleOpSKVI

      val oneBaseTable = store.getBaseTables().first()
      print("On $oneBaseTable. ")
      val tos = TupleOpSetting(store, oneBaseTable, ac)
      val itset: IteratorSetting = TupleOpSKVI.iteratorSetting(TupleOpSerializer.INSTANCE, tos, 25)

      ac.connector.createBatchScanner(oneBaseTable, Authorizations.EMPTY, 15).use { bs ->
        val ranges = listOf(Range())
        bs.setRanges(ranges)
        bs.addScanIterator(itset)

        println("Execute $store")
        bs.iterator().forEach { (k, v) -> println("${k.toStringNoTime()} -> $v") }
      }
    }

    println()
    Cpipelines.forEach(pipelineFun)
    val finalStore = Cpipelines.last()
    printTable(finalStore.table, finalStore.resultSchema.defaultPSchema())
    // todo - make a PrintTupleOp for debugging purposes
    // then can run it ClientSide if we like, after adapting KeyValues to Tuples :)
  }

  fun printTable(table: String, ps: PSchema) {
    tester.accumuloConfig.connector.createScanner(table, Authorizations.EMPTY).use { scanner ->
      scanner.iterator().forEach { (k,v) ->
        println(TupleByKeyValue(ps, k, v))
//        println("${k.toStringNoTime()} -> ${PType.DOUBLE.nullable.decode(v.get())}")
      }
    }

  }



}