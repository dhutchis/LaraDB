package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.*
import edu.washington.cs.laragraphulo.opt.*
import edu.washington.cs.laragraphulo.opt.AccumuloConfig.Companion.PROP_PSCHEMA
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import edu.washington.cs.laragraphulo.util.SkviToIteratorAdapter
import org.slf4j.Logger
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.OptionDescriber
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import java.io.Serializable
import java.util.*


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
    val tupleOp: TupleOp,
    /** Name of the table that we're scanning over. */
    val thisTable: Table,
    val accumuloConfig: AccumuloConfig
) : Serializable

class TupleOpSKVI : DelegatingIterator(), OptionDescriber {
  companion object : SerializerSetting<TupleOpSetting>(TupleOpSKVI::class.java), Loggable {
//    const val OPT_THIS_TABLE = "THIS_TABLE"
    override val logger: Logger = logger<TupleOpSKVI>()
  }

  override fun initDelegate(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment): SKVI {
    val (store,thisTable,accumuloConfig) = deserializeFromOptions(options)

    // replace Load statements with this iterator and RemoteSourceIterators
    val baseTables = store.getBaseTables()
    if (thisTable !in baseTables) logger.warn{"thisTable $thisTable !in baseTables: $baseTables for query $store"}
    // get schemas of base tables - assume all the schemas are set.
    val baseTablesSchemas = baseTables.map { table ->
//      logger.debug{"Fetch Schema for $table"}
      if (table == thisTable)
        try {
          val ser = env.config.get(PROP_PSCHEMA)
          val ps = SerializationUtil.deserializeBase64(ser) as PSchema
          logger.info{"Schema for $table is $ps"}
          table to ps
        } catch(e: Exception) {
          logger.warn{"Cannot fetch schema from environment for $table"}
          // We should not need to use this method. It is less reliable because table information
          // may be cached or inconsistent, leading to TableNotFoundException, even though we execute on this table.
          table to accumuloConfig.getSchema(table)
        }
      else table to accumuloConfig.getSchema(table)
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
      logger.debug{"Schema for table $table is $ps on skvi $skvi${if (skvi is DelegatingIterator) "; ${skvi.unwrap()}" else ""}"}
      KvToTupleAdapter(ps, SkviToKvAdapter(skvi))
    }
    // also change Sorts to do nothing
    val storeLoaded = store.instantiateLoadTupleIterator(tupleIters).disableFullResorts()
    logger.info{"Loading TupleOp: $storeLoaded"}

    //  instantiate Stores with remote write iterators
    // for now, only check if the last operator is a Store
    if (storeLoaded is TupleOp.Store) {
      val runBeforeStore: TupleIterator = storeLoaded.p.run(env)
      logger.info{"Loading TupleOpIterator: $runBeforeStore"}
      logger.debug{"Schema to store in ${storeLoaded.table} is ${storeLoaded.resultSchema}"}
      val skviBeforeStore = KvToSkviAdapter(TupleToKvAdapter(storeLoaded.resultSchema.defaultPSchema(), runBeforeStore))

      val remoteOpts = accumuloConfig.basicRemoteOpts(remoteTable = storeLoaded.table)
      val rwi = RemoteWriteIterator()
      rwi.init(skviBeforeStore, remoteOpts, env)
      return rwi
    } else {
      val ti = storeLoaded.run(env)
      return KvToSkviAdapter(TupleToKvAdapter(storeLoaded.resultSchema.defaultPSchema(), ti))
    }
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


/** Execute a query on the Accumulo pointed to by this AccumuloConfig */
fun AccumuloConfig.execute(query: TupleOp.Store): Long {
  println("Execute Query: $query")
  val pipelines: List<TupleOp.Store> = query.splitPipeline()
  print("Pipelines to execute: ")
  println(pipelines.joinToString("\n","[\n","\n]"))

  val totalEntries = pipelines.map {
    val oneBaseTable = it.getBaseTables().first()
    print("On $oneBaseTable: ")
    val tos = TupleOpSetting(it, oneBaseTable, this)
    tos.executeSingle()
  }.reduce(Long::plus)

  println("[[[$totalEntries total entries for pipeline]]]")
  return totalEntries
}

/** Execute a single query fragment on the Accumulo pointed to by this AccumuloConfig */
fun TupleOpSetting.executeSingle(priority: Int = 25): Long {
  val ac = this.accumuloConfig
  val itset: IteratorSetting = TupleOpSKVI.iteratorSetting(TupleOpSerializer.INSTANCE, this, priority)
  val store = this.tupleOp

  if (store is TupleOp.Store) {
    print("Create ${store.table}. ")
    GraphuloUtil.recreateTables(ac.connector, false, store.table)
    ac.setSchema(store.table, store.resultSchema.defaultPSchema()) // matches that in TupleOpSKVI
    println("Schema ${store.resultSchema.defaultPSchema()}. ")
    if (store.aggMap.isNotEmpty()) {
      // we are summing into a possibly existing table with entries
      // I assume the new entries match the schema of the existing entries
      // the agg iterator should decode entries (both new and old) the same way,
      // and mergeunion them
      val aggtop = TupleOp.Load(store.table, store.resultSchema)
          .agg(store.resultSchema.keys.map(Attribute<*>::name), store.aggMap)
//          .log()
      val names = store.aggMap.map { (_,agg) -> agg.name }.joinToString("_")
      val aggset = TupleOpSetting(aggtop, store.table, ac)
      aggset.attachIterator(2, "Aggregation_$names")
    }
  }

  return ac.connector.createBatchScanner(this.thisTable, Authorizations.EMPTY, 15).use { bs ->
    val ranges = listOf(Range())
    bs.setRanges(ranges)
    bs.addScanIterator(itset)
    var totalEntries = 0L
    var count = 0

    println("Execute ${this.tupleOp}")
    bs.iterator().forEach { (k, v) ->
      val numEntries = RemoteWriteIterator.decodeValue(v, null)
      println("[$numEntries entries] ${k.toStringNoTime()} -> $v")
      totalEntries += numEntries
      count++
    }
    if (count != 1)
      println("[[$totalEntries total entries]]")
    totalEntries
  }
}

/** Attach a TupleOp to an Accumulo table. */
fun TupleOpSetting.attachIterator(priority: Int = 22, name: String = TupleOpSKVI::class.java.simpleName,
                                  scopes: EnumSet<IteratorUtil.IteratorScope> = EnumSet.allOf(IteratorUtil.IteratorScope::class.java)) {
  val ac = this.accumuloConfig
  val itset = TupleOpSKVI.iteratorSetting(TupleOpSerializer.INSTANCE, this, priority, name)
  GraphuloUtil.addOnScopeOption(itset, scopes)
  val store = this.tupleOp

  if (store is TupleOp.Store) {
    print("Create ${store.table}. ")
    ac.connector.tableOperations().create(store.table)
    ac.setSchema(store.table, store.resultSchema.defaultPSchema()) // matches that in TupleOpSKVI
    println("Schema ${store.resultSchema.defaultPSchema()}. ")
  }

  GraphuloUtil.applyIteratorSoft(itset, ac.connector.tableOperations(), this.thisTable)
}


fun AccumuloConfig.ingestData(table: Table, ps: PSchema, data: Iterable<Tuple>, deleteIfExists: Boolean) {
  GraphuloUtil.recreateTables(this.connector, deleteIfExists, table)
  val iter = TupleToKvAdapter(ps, TupleIterator.DataTupleIterator(ps, data))
  this.setSchema(table, ps)
  this.connector.createBatchWriter(table, BatchWriterConfig().setMaxWriteThreads(15)).use { bw ->
    var r: ByteSequence = ArrayByteSequence(byteArrayOf())
    var m = Mutation(byteArrayOf())
    for ((k, v) in iter) {
      val kr = k.rowData
      if (kr != r) {
        if (m.size() > 0) bw.addMutation(m)
        m = Mutation(kr.toArray())
        r = kr
      }
      m.put(k.columnFamily, k.columnQualifier, k.columnVisibilityParsed, k.timestamp, v)
    }
    if (m.size() > 0) bw.addMutation(m)
  }
}

fun AccumuloConfig.scanAccumulo(
    table: Table, range: Range = Range()
): Iterator<Tuple> {
  val ps = this.getSchema(table)
  val scanner = this.connector.createScanner(table, Authorizations.EMPTY)
  scanner.range = range
  return KvToTupleAdapter(ps, scanner.asKvIterator())
}

fun AccumuloConfig.clone(table1: Table, table2: Table) {
  this.connector.tableOperations().clone(table1, table2,
      true, null, null)
}

fun AccumuloConfig.exists(table: Table) = this.connector.tableOperations().exists(table)

