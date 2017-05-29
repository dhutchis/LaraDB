package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.opt.*
import edu.washington.cs.laragraphulo.util.SkviToIteratorAdapter
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.OptionDescriber
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import java.io.Serializable


class TupleOpSerializer : Serializer<TupleOpSetting, TupleOpSetting> {
  override fun serializeToString(obj: TupleOpSetting): String =
      SerializationUtil.serializeBase64(obj)
  @Suppress("UNCHECKED_CAST")
  override fun deserializeFromString(str: String): TupleOpSetting =
      SerializationUtil.deserializeBase64(str) as TupleOpSetting
  companion object {
    val INSTANCE = TupleOpSerializer()
  }
}
/** This is what we need to run a TupleOp stack in Accumulo. */
data class TupleOpSetting(
    val tupleOp: TupleOp.Store,
    /** Name of the table that we're scanning over. */
    val thisTable: Table,
    val accumuloConfig: AccumuloConfig
) : Serializable

class TupleOpSKVI : DelegatingIterator(), OptionDescriber {
  companion object : SerializerSetting<TupleOpSetting>(TupleOpSKVI::class.java) {
//    const val OPT_THIS_TABLE = "THIS_TABLE"
  }

  override fun initDelegate(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment): SKVI {
    val (store,thisTable,accumuloConfig) = deserializeFromOptions(options)

    // replace Load statements with this iterator and RemoteSourceIterators
    val baseTables = store.getBaseTables()

    // get schemas of base tables - assume all the schemas are set.
    val baseTablesSchemas = baseTables.map { table ->
      table to accumuloConfig.getSchema(table)
    }.toMap()

    // what about the PhysicalSchemas?
    // store the PSchema of each table as a table config entry
    // need to decide on physical schemas of pipeline output
    // ^-- use default pschema - don't worry about the grouping problem right now

    val tupleIters: Map<Table, TupleIterator> = baseTablesSchemas.mapValues { (table,ps) ->
      val skvi = if (table == thisTable) source else {
        // create RemoteSourceIterators for each other base table and wrap them to be TupleIterators
        val remoteOpts = accumuloConfig.basicRemoteOpts(remoteTable = table)
        val rsi = RemoteSourceIterator()
        rsi.init(null, remoteOpts, env)
        rsi
      }
      KvToTupleAdapter(ps, SkviToIteratorAdapter(skvi))
    }
    // also change Sorts to do nothing
    val storeLoaded = store.instantiateLoadTupleIterator(tupleIters).disableFullResorts() as TupleOp.Store

    //  instantiate Stores with remote write iterators
    // for now, assume there is only one Store and it is the last one
    val runBeforeStore: TupleIterator = storeLoaded.p.run(env)
    val skviBeforeStore = KvToSkviAdapter(TupleToKvAdapter(storeLoaded.resultSchema.defaultPSchema(), runBeforeStore))

    val remoteOpts = accumuloConfig.basicRemoteOpts(remoteTable = store.table)
    val rwi = RemoteWriteIterator()
    rwi.init(skviBeforeStore, remoteOpts, env)
    return rwi
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    return OptionDescriber.IteratorOptions("TupleOpSKVI",
        "constructs a new Serializer<TupleOp>, uses it to deserialize a TupleOp payload, " +
            "and constructs a TupleIterator stack out of it",
        mapOf(SerializerSetting.OPT_SERIALIZED_DATA to "the serialized TupleOp",
            SerializerSetting.OPT_SERIALIZER_CLASS to "the class that can deserialize the TupleOp; " +
                "must have a no-args constructor"),
//            OPT_THIS_TABLE to "the name of the table that this scan is attached to"),
        null)
  }
  override fun validateOptions(options: Map<String, String>): Boolean {
    deserializeFromOptions(options)
//    if (OPT_THIS_TABLE !in options) throw IllegalArgumentException("no $OPT_THIS_TABLE")
    return true
  }
}


/** Execute a qury on the Accumulo pointed to by this AccumuloConfig */
fun AccumuloConfig.execute(query: TupleOp.Store) {
  val ac = this

  val pipelineFun: (TupleOp.Store) -> Unit = { store ->
    // create the table we want to store to and set its schema
//      val ntc = NewTableConfiguration().setProperties()
    print("Create ${store.table}. ")
    println("Schema ${store.resultSchema.defaultPSchema()}. ")
    ac.connector.tableOperations().create(store.table)
    ac.setSchema(store.table, store.resultSchema.defaultPSchema()) // matches that in TupleOpSKVI

    val oneBaseTable = store.getBaseTables().first()
    print("On $oneBaseTable: ")
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

  println("Execute Query: $query")
  val pipelines: List<TupleOp.Store> = query.splitPipeline()
  print("Pipelines to execute: ")
  println(pipelines.joinToString("\n","[\n","\n]"))

  pipelines.forEach(pipelineFun)
}

