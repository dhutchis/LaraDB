package edu.washington.cs.laragraphulo.opt


import edu.washington.cs.laragraphulo.AccumuloTestBase
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import edu.washington.cs.laragraphulo.opt.raco.PTree
import edu.washington.cs.laragraphulo.opt.raco.RacoOperator
import edu.washington.cs.laragraphulo.opt.raco.RelationKey
import edu.washington.cs.laragraphulo.util.DebugUtil
import kotlinx.support.jdk7.use
import org.apache.accumulo.core.security.Authorizations
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.slf4j.Logger
import java.io.File
import java.io.StringReader
import java.util.concurrent.Callable

//import edu.washington.cs.laragraphulo.*

/**
 * Start with a Raco query, compile it, and execute the result.
 */
@RunWith(Parameterized::class)
class QueryOptimizeExecuteTest(
    val params: Params
) : AccumuloTestBase() {


  data class Params (
      val name: String,
      val query: List<String>,
      val beforeTasks: (AccumuloConfig) -> List<Callable<*>> = {listOf()},
      val afterTasks: (AccumuloConfig) -> List<Callable<*>> = {listOf()}
  ) {
    override fun toString(): String = name
  }


  @Test
  fun test() {
    println("TEST: ${params.name}")
    params.beforeTasks.invoke(tester.accumuloConfig).forEach { it.call() }

    params.query.forEach { query ->
      println("QUERY: $query")
      val ptree = StringReader(query).use { PTree.parseRaco(it) }
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
            serializer as Serializer<Op<SKVI>, Op<SKVI>>

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
      println()
    }

    params.afterTasks.invoke(tester.accumuloConfig).forEach { it.call() }
  }



  companion object : Loggable {
    override val logger: Logger = logger<QueryOptimizeExecuteTest>()

    val netflow_sample_file: File?

    /*
T1 = load("file://...<snip>....data/netflow/botnet-capture-20110810-neris.pcap.subset.txt",
csv(schema(StartTime:string,Dur:float,Proto:string,SrcAddr:string,Sport:string,Dir:string,DstAddr:string,Dport:string,State:string,sTos:int,dTos:int,TotPkts:int,TotBytes:float,SrcBytes:int,Label:string,__DAP__:int,__LAP__:int),skip=1));
T2 = select TotBytes, StartTime, __DAP__, __LAP__, SrcAddr, DstAddr, (TotBytes / Dur) / 1000.0 as RATE, Dur, Dir, Proto, Sport, Dport, State, sTos, dTos, TotPkts, SrcBytes, Label from T1;
store(T2, netflow_subset);

cd gits/raco/scripts
git checkout repr-dump
./myrial test.myl --catalog ../examples/catalog.py -L -r


T1 = load("file://...<snip>....data/netflow/botnet-capture-20110810-neris.pcap.subset.txt",
csv(schema(StartTime:string,Dur:float,Proto:string,SrcAddr:string,Sport:string,Dir:string,DstAddr:string,Dport:string,State:string,sTos:int,dTos:int,TotPkts:int,TotBytes:float,SrcBytes:int,Label:string,__DAP__:int,__LAP__:int),skip=1));
T2 = select TotBytes, StartTime, __DAP__, __LAP__, SrcAddr, DstAddr, (TotBytes / Dur) / 1000.0 as RATE, Dur, Dir, Proto, Sport, Dport, State, sTos, dTos, TotPkts, SrcBytes, Label from T1
where TotBytes > 250;
store(T2, netflow_subset);


Store(RelationKey('public','adhoc','netflow_subset'),
 Apply([('TotBytes', NamedAttributeRef('TotBytes')), ('StartTime', NamedAttributeRef('StartTime')), ('__DAP__', NamedAttributeRef('__DAP__')), ('__LAP__', NamedAttributeRef('__LAP__')), ('SrcAddr', NamedAttributeRef('SrcAddr')), ('DstAddr', NamedAttributeRef('DstAddr')),
 ('RATE', DIVIDE(DIVIDE(NamedAttributeRef('TotBytes'), NamedAttributeRef('Dur')), NumericLiteral(1000.0))),
 ('Dur', NamedAttributeRef('Dur')), ('Dir', NamedAttributeRef('Dir')), ('Proto', NamedAttributeRef('Proto')), ('Sport', NamedAttributeRef('Sport')), ('Dport', NamedAttributeRef('Dport')), ('State', NamedAttributeRef('State')), ('sTos', NamedAttributeRef('sTos')), ('dTos', NamedAttributeRef('dTos')), ('TotPkts', NamedAttributeRef('TotPkts')), ('SrcBytes', NamedAttributeRef('SrcBytes')), ('Label', NamedAttributeRef('Label'))
 ],
  Select(GT(UnnamedAttributeRef(12, None), NumericLiteral(250)),
   FileScan('file://...<snip>....data/netflow/botnet-capture-20110810-neris.pcap.subset.txt', 'CSV',
    Scheme([('StartTime', 'STRING_TYPE'), ('Dur', 'DOUBLE_TYPE'), ('Proto', 'STRING_TYPE'), ('SrcAddr', 'STRING_TYPE'), ('Sport', 'STRING_TYPE'), ('Dir', 'STRING_TYPE'), ('DstAddr', 'STRING_TYPE'), ('Dport', 'STRING_TYPE'), ('State', 'STRING_TYPE'), ('sTos', 'LONG_TYPE'), ('dTos', 'LONG_TYPE'), ('TotPkts', 'LONG_TYPE'), ('TotBytes', 'DOUBLE_TYPE'), ('SrcBytes', 'LONG_TYPE'), ('Label', 'STRING_TYPE'), ('__DAP__', 'LONG_TYPE'), ('__LAP__', 'LONG_TYPE')]),
    {'skip': 1}
))))


     */

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
            "('sTos', 'LONG_TYPE')," +
            "('dTos', 'LONG_TYPE')," +
            "('TotPkts', 'LONG_TYPE')," +
            "('TotBytes', 'LONG_TYPE')," +
            "('SrcBytes', 'LONG_TYPE')," +
            "('Label', 'STRING_TYPE')" +
            "]), {'skip': 1})"

    val netflow_sample_scheme_daplap: String =
        "Scheme([" +
            "('TotBytes', 'LONG_TYPE')," +
            "('StartTime', 'STRING_TYPE'), " +
            "('$__DAP__', 'STRING_TYPE'), " +
            "('$__LAP__', 'STRING_TYPE'), " +
            "('SrcAddr', 'STRING_TYPE')," +
            "('DstAddr', 'STRING_TYPE')," +
            "('RATE', 'DOUBLE_TYPE')," +
            "('Dur', 'DOUBLE_TYPE')," +
            "('Dir', 'STRING_TYPE')," +
            "('Proto', 'STRING_TYPE')," +
            "('Sport', 'STRING_TYPE')," +
            "('Dport', 'STRING_TYPE')," +
            "('State', 'STRING_TYPE')," +
            "('sTos', 'LONG_TYPE')," +
            "('dTos', 'LONG_TYPE')," +
            "('TotPkts', 'LONG_TYPE')," +
            "('SrcBytes', 'LONG_TYPE')," +
            "('Label', 'STRING_TYPE')" +
            "])"

    val tests: Array<Params>

    init {
      val resourcePath = "data/netflow/botnet-capture-20110810-neris.pcap.subset.txt"
      netflow_sample_file =
//          File("/home/dhutchis/gits/raco/examples/fed_accumulo_spark_c/captureSmall")
          Thread.currentThread().contextClassLoader.
          getResource(resourcePath)?.let {
        File(it.path)
      } ?: null
      if (netflow_sample_file != null) {
        val fileurl = "file://"+netflow_sample_file.absolutePath
        val filescan = "FileScan('$fileurl', 'CSV', $netflow_sample_scheme)"

        val fileStoreUrl = "${netflow_sample_file.absolutePath}.out.csv"

//        val fileStoreFederated = "/home/dhutchis/gits/raco/raco/backends/federated/tests/V5407830105"

        tests = arrayOf<Params>(
//            Params(
//                name = "store apply filescan Named",
//                query = listOf("Store(RelationKey('public','adhoc','netflow_subset'), " +
//                    "Apply([('TotBytes', NamedAttributeRef('TotBytes'))," +
//                    "('StartTime', NamedAttributeRef('StartTime'))," +
//                    "('$__DAP__', NumericLiteral(0))," +
//                    "('$__LAP__', NumericLiteral(0))," +
//                    "('SrcAddr', NamedAttributeRef('DstAddr'))," +
//                    "('DstAddr', NamedAttributeRef('DstAddr'))," +
//                    "('RATE', DIVIDE(DIVIDE(NamedAttributeRef('TotBytes'), NamedAttributeRef('Dur')), NumericLiteral(1000.0)))," +
//                    "('Dur', NamedAttributeRef('Dur'))," +
//                    "('Dir', NamedAttributeRef('Dir'))," +
//                    "('Proto', NamedAttributeRef('Proto'))," +
//                    "('Sport', NamedAttributeRef('Sport'))," +
//                    "('Dport', NamedAttributeRef('Dport'))," +
//                    "('State', NamedAttributeRef('State'))," +
//                    "('sTos', NamedAttributeRef('sTos'))," +
//                    "('dTos', NamedAttributeRef('dTos'))," +
//                    "('TotPkts', NamedAttributeRef('TotPkts'))," +
//                    "('SrcBytes', NamedAttributeRef('SrcBytes'))," +
//                    "('Label', NamedAttributeRef('Label'))" +
//                    "], " +
//                    "$filescan))",
//
//                    "Store(RelationKey('public','adhoc','netflow_subset2'), " +
//                        "Select(GT(NamedAttributeRef('TotBytes'), NumericLiteral(500)), " +
//                        "Scan(RelationKey('public','adhoc','netflow_subset')," +
//                        "$netflow_sample_scheme_daplap, 500, RepresentationProperties(frozenset([]), None, None))))",
//
//                    "FileStore('$fileStoreUrl', 'CSV', {'header': 'true'}, " +
//                        "Select(GT(NamedAttributeRef('TotBytes'), NumericLiteral(500)), " +
//                        "Scan(RelationKey('public','adhoc','netflow_subset')," +
//                        "$netflow_sample_scheme_daplap, 500, RepresentationProperties(frozenset([]), None, None))))"
//                ),
//                beforeTasks = { listOf() },
//                afterTasks = { config -> listOf(
//                    Callable {
//                      val stored_table = RelationKey("public","adhoc","netflow_subset").sanitizeTableName()
//                      val stored_table2 = RelationKey("public","adhoc","netflow_subset2").sanitizeTableName()
//
//                      listOf(stored_table, stored_table2).forEach { table ->
//                        config.connector.createScanner(table, Authorizations.EMPTY).use {
//                          for ((key, value) in it) {
//                            println(key.toStringNoTime() + " --> " + value)
//                          }
//                        }
//                        DebugUtil.printTable(table, config.connector, table, 15)
//                        println()
//                        println()
//                      }
//
//                      val file = File(fileStoreUrl)
//                      assertTrue(file.exists())
//                      println("Reading $file:")
//                      file.reader().buffered().useLines { it.forEach { line ->
//                          println(line)
//                      } }
//                    }
//                ) }
//            ),

            Params(
                name = "from federated",
                query = listOf(
                    "Store(RelationKey('public','adhoc','netflow'), " +
                    "Apply([('TotBytes', NamedAttributeRef('TotBytes'))," +
                    "('StartTime', NamedAttributeRef('StartTime'))," +
                    "('$__DAP__', NumericLiteral(0))," +
                    "('$__LAP__', NumericLiteral(0))," +
                    "('SrcAddr', NamedAttributeRef('SrcAddr'))," +
                    "('DstAddr', NamedAttributeRef('DstAddr'))," +
                    "('RATE', DIVIDE(DIVIDE(NamedAttributeRef('TotBytes'), NamedAttributeRef('Dur')), NumericLiteral(1000.0)))," +
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

                    "FileStore('$fileStoreUrl', 'CSV', {}, " +
                        "Apply([" +
                        "('src_ip', NamedAttributeRef('SrcAddr')), " +
                        "('dst_ip', NamedAttributeRef('DstAddr')), " +
                        "('value', NumericLiteral(1.0))], " +
                        "Select(GT(UnnamedAttributeRef(0, None), NumericLiteral(100)), " +
                        "Scan(RelationKey('public','adhoc','netflow'), " +
                        "Scheme([(u'TotBytes', 'LONG_TYPE'), " +
                        "(u'StartTime', 'STRING_TYPE'), " +
                        "(u'SrcAddr', 'STRING_TYPE'), " +
                        "(u'DstAddr', 'STRING_TYPE'), " +
                        "(u'RATE', 'DOUBLE_TYPE'), " +
                        "(u'Dur', 'DOUBLE_TYPE'), " +
                        "(u'Dir', 'STRING_TYPE'), " +
                        "(u'Proto', 'STRING_TYPE'), " +
                        "(u'Sport', 'STRING_TYPE'), " +
                        "(u'Dport', 'STRING_TYPE'), " +
                        "(u'State', 'STRING_TYPE'), " +
                        "(u'sTos', 'LONG_TYPE'), " +
                        "(u'dTos', 'LONG_TYPE'), " +
                        "(u'TotPkts', 'LONG_TYPE'), " +
                        "(u'SrcBytes', 'LONG_TYPE'), " +
                        "(u'Label', 'STRING_TYPE')]), " +
                        "(<raco.backends.myria.catalog.MyriaCatalog object at 0x7f207134fbd0>, 7), " +
                        "RepresentationProperties(frozenset([UnnamedAttributeRef(0, None), UnnamedAttributeRef(1, None)]), None, None)" +
                        "))))"
                ),
                beforeTasks = { listOf() },
                afterTasks = { config -> listOf(
                    Callable {
                      val stored_table = RelationKey("public","adhoc","netflow").sanitizeTableName()

//                      listOf(stored_table).forEach { table ->
//                        config.connector.createScanner(table, Authorizations.EMPTY).use {
//                          for ((key, value) in it) {
//                            println(key.toStringNoTime() + " --> " + value)
//                          }
//                        }
//                        DebugUtil.printTable(table, config.connector, table, 15)
//                        println()
//                        println()
//                      }

                      val file = File(fileStoreUrl)
                      assertTrue(file.exists())
                      println("Reading $file:")
                      file.reader().buffered().useLines { it.forEach { line ->
                          println(line)
                      } }
                      println()
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
