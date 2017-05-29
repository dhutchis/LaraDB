package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.examples.SensorExample
import edu.washington.cs.laragraphulo.sensor.EncodeFunction
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.junit.Test


class APIExecuteTest : AccumuloTestBase() {

  @Test
  fun test() {
    // Ingest sensor data
    val ac = tester.accumuloConfig
    SensorExample.ingestSensor(ac.connector, SensorExample.filepathA, "sensorA", EncodeFunction.PTYPE)
    SensorExample.ingestSensor(ac.connector, SensorExample.filepathB, "sensorB", EncodeFunction.PTYPE)

    // Set Physical Schema of sensor data to (row=t, colq=c, val=v)
    val aT = SensorQuery.attrT.defaultPhysical()
    val aC = SensorQuery.attrC.defaultPhysical()
    val aV = SensorQuery.attrVn.defaultPhysical()
    val sensorSchema = PSchema(listOf(aT), colq = listOf(aC), pvals = listOf(aV))
    ac.setSchema("sensorA", sensorSchema)
    ac.setSchema("sensorB", sensorSchema)

    val query = TupleOp.Store(SensorQuery.C, "sensorC")

    ac.execute(query)

    printTable(query.table, query.resultSchema.defaultPSchema())
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