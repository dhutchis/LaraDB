package edu.washington.cs.laragraphulo.opt


import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.opt.*
import edu.washington.cs.laragraphulo.opt.raco.*
import edu.washington.cs.laragraphulo.opt.raco.RacoConvertTest.Companion.toABS
import org.apache.accumulo.core.data.ArrayByteSequence
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.StringReader
import java.nio.ByteBuffer
import edu.washington.cs.laragraphulo.opt.viz.generateDot
import edu.washington.cs.laragraphulo.util.DebugUtil
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import edu.washington.cs.laragraphulo.util.TestUtil
import kotlinx.support.jdk7.use
import org.apache.accumulo.core.security.Authorizations
import org.junit.Assume.*
import org.slf4j.Logger
import java.io.File
import java.net.URL
import java.util.concurrent.Callable
//import edu.washington.cs.laragraphulo.*

/**
 * Start with a Raco query, compile it, and execute the result.
 */
@RunWith(Parameterized::class)
class QueryOptimizeExecuteIT(
    val params: Params
) : AccumuloTestBase() {


  data class Params (
      val name: String,
      val query: String,
      val beforeTasks: (AccumuloConfig) -> List<Callable<*>> = {listOf()},
      val afterTasks: (AccumuloConfig) -> List<Callable<*>> = {listOf()}
  ) {
    override fun toString(): String = name
  }


  @Test
  fun test() {
    println("TEST: ${params.name}")
    params.beforeTasks.invoke(tester.accumuloConfig).forEach { it.call() }

    println("QUERY: ${params.query}")
    val ptree = StringReader(params.query).use { PTree.parseRaco(it) }
    println("PTREE: $ptree")
    val racoOp = RacoOperator.parsePTreeToRacoTree(ptree)
    println("RacoOp: $racoOp")
    val callables = executorsRacoOnAccumulo(racoOp, tester.accumuloConfig)
    println("Callables : $callables")

    callables.forEachIndexed { i, callable ->
      print("$i: ")
      @Suppress("UNCHECKED_CAST")
      when (callable) {
        is CreateTableTask -> {
          println("CreateTableTask(${callable.tableName})")
          callable.call()
        }
        is AccumuloPipelineTask<*> -> {
          val table = callable.pipeline.tableName
          val serializer = callable.pipeline.serializer
          val skvi = callable.pipeline.data
          skvi as Op<SKVI>
          serializer as Serializer<Op<SKVI>,Op<SKVI>>

          println("AccumuloPipelineTask($table): $skvi")
//          println("dot:\n${skvi.generateDot()}")
//          val serialized = serializer.serializeToString(skvi)
//          val deserialized = serializer.deserializeFromString(serialized)

          callable.call()
        }
        else -> {
          println("???: $callable")
        }
      }
    }

    params.afterTasks.invoke(tester.accumuloConfig).forEach { it.call() }
  }



  companion object : Loggable {
    override val logger: Logger = logger<QueryOptimizeExecuteIT>()

    val netflow_sample_file: File?

    val netflow_sample_scheme: String =
        "Scheme([" +
            "('StartTime', 'STRING_TYPE'), " +
            "('Dur', 'DOUBLE_TYPE')," +
            "('Proto', 'STRING_TYPE')," +
            "('SrcAddr', 'STRING_TYPE')," +
            "('Sport', 'STRING_TYPE')," +
            "('Dir', 'STRING_TYPE')," +
            "('DstAddr', 'STRING_TYPE')," +
            "('Dport', 'STRING_TYPE')," +
            "('State', 'STRING_TYPE')," +
            "('sTos', 'INT_TYPE')," +
            "('dTos', 'INT_TYPE')," +
            "('TotPkts', 'INT_TYPE')," +
            "('TotBytes', 'INT_TYPE')," +
            "('SrcBytes', 'INT_TYPE')," +
            "('Label', 'STRING_TYPE')" +
            "]), {'skip': 1})"

    val tests: Array<Params>

    init {
      val resourcePath = "data/netflow/botnet-capture-20110810-neris.pcap.subset.txt"
      netflow_sample_file = Thread.currentThread().contextClassLoader.
          getResource(resourcePath)?.let {
        File(it.path)
      } ?: null
      if (netflow_sample_file != null) {
        val fileurl = "file://"+netflow_sample_file.absolutePath
        val filescan = "FileScan('$fileurl', 'CSV', $netflow_sample_scheme)"

        tests = arrayOf<Params>(
            Params(
                name = "store apply filescan Named",
                query = "Store(RelationKey('public','adhoc','netflow_subset'), " +
                    "Apply([('TotBytes', NamedAttributeRef('TotBytes'))," +
                    "('StartTime', NamedAttributeRef('StartTime'))," +
                    "('$__DAP__', NumericLiteral(0))," +
                    "('$__LAP__', NumericLiteral(0))," +
                    "('SrcAddr', NamedAttributeRef('DstAddr'))," +
                    "('DstAddr', NamedAttributeRef('DstAddr'))," +
                    "('RATE', NamedAttributeRef('DstAddr'))," +
                    "('Dur', NamedAttributeRef('Dur'))," +
                    "('Dir', NamedAttributeRef('Dir'))," +
                    "('Proto', NamedAttributeRef('Proto'))," +
                    "('Sport', NamedAttributeRef('Sport'))," +
                    "('Dport', NamedAttributeRef('Dport'))," +
                    "('State', NamedAttributeRef('State'))," +
                    "('sTos', NamedAttributeRef('sTos'))," +
                    "('dTos', NamedAttributeRef('dTos'))," +
                    "('TotPkts', NamedAttributeRef('TotPkts'))," +
                    "('SrcBytes', NamedAttributeRef('SrcBytes'))," +
                    "('Label', NamedAttributeRef('Label'))" +
                    "], " +
                    "$filescan))",
                beforeTasks = { listOf() },
                afterTasks = { config -> listOf(
                    Callable {
                      val stored_table = RelationKey("public","adhoc","netflow_subset").sanitizeTableName()

                      config.connector.createScanner(stored_table, Authorizations.EMPTY).use {
                        for ((key, value) in it) {
                          println(key.toStringNoTime() + " --> " + value)
                        }
                      }
                      DebugUtil.printTable(stored_table, config.connector, stored_table, 15)
                    }
                ) }
            )
        )
      } else {
        logger.error("Cannot find file at $resourcePath; no tests will run")
        tests = arrayOf()
      }
    }

    // StartTime,Dur,Proto,SrcAddr,Sport,Dir,DstAddr,Dport,State,sTos,dTos,TotPkts,TotBytes,SrcBytes,Label
    // 2011/08/10 09:46:59.607825,1.026539,tcp,94.44.127.113,1577,   ->,147.32.84.59,6881,S_RA,0,0,4,276,156,flow=Background-Established-cmpgw-CVUT



    @JvmStatic
    @Parameterized.Parameters(name = "test {index}: {0}")
    fun parameters(): Array<out Any> = tests

  }

}
