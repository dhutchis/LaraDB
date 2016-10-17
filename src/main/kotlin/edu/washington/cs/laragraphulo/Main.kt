package edu.washington.cs.laragraphulo

import com.beust.jcommander.Parameter
import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import edu.washington.cs.laragraphulo.opt.AccumuloConfigImpl
import edu.washington.cs.laragraphulo.opt.executeTasksSerial
import edu.washington.cs.laragraphulo.opt.executorsRacoOnAccumulo
import edu.washington.cs.laragraphulo.opt.raco.PTree
import edu.washington.cs.laragraphulo.opt.raco.RacoOperator
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.slf4j.Logger
import java.util.*

class Main {

  // maybe later adapt this from ShellOptionsJC in Accumulo's shell
  private class Opts : Help() {
//    @Parameter(names = arrayOf("-oTsampleSeqRaw"))
//    var oTsampleSeqRaw = "oTsampleSeqRaw"

    @Parameter(description = "the query to execute")
    var query: List<String> = ArrayList()


  }


  companion object : Loggable {
    override val logger: Logger = logger<Main>()


    /**
     * ```
     * java -jar /home/dhutchis/gits/lara-graphulo/target/lara-graphulo-1.0-SNAPSHOT-all.jar edu.washington.cs.laragraphulo.Main QUERY
     *
     * java -cp /home/dhutchis/gits/lara-graphulo/target/lara-graphulo-1.0-SNAPSHOT-all.jar edu.washington.cs.laragraphulo.Main "Store(RelationKey('public','adhoc','netflow'), Apply([('TotBytes', NamedAttributeRef('TotBytes')),('StartTime', NamedAttributeRef('StartTime')),('__DAP__', NumericLiteral(0)),('__LAP__', NumericLiteral(0)),('SrcAddr', NamedAttributeRef('SrcAddr')),('DstAddr', NamedAttributeRef('DstAddr')),('RATE', DIVIDE(DIVIDE(NamedAttributeRef('TotBytes'), NamedAttributeRef('Dur')), NumericLiteral(1000.0))),('Dur', NamedAttributeRef('Dur')),('Dir', NamedAttributeRef('Dir')),('Proto', NamedAttributeRef('Proto')),('Sport', NamedAttributeRef('Sport')),('Dport', NamedAttributeRef('Dport')),('State', NamedAttributeRef('State')),('sTos', NamedAttributeRef('sTos')),('dTos', NamedAttributeRef('dTos')),('TotPkts', NamedAttributeRef('TotPkts')),('SrcBytes', NamedAttributeRef('SrcBytes')),('Label', NamedAttributeRef('Label'))], FileScan('file:///home/dhutchis/gits/lara-graphulo/target/test-classes/data/netflow/botnet-capture-20110810-neris.pcap.subset.txt', 'CSV', Scheme([('StartTime', 'STRING_TYPE'), ('Dur', 'DOUBLE_TYPE'),('Proto', 'STRING_TYPE'),('SrcAddr', 'STRING_TYPE'),('Sport', 'STRING_TYPE'),('Dir', 'STRING_TYPE'),('DstAddr', 'STRING_TYPE'),('Dport', 'STRING_TYPE'),('State', 'STRING_TYPE'),('sTos', 'INT_TYPE'),('dTos', 'INT_TYPE'),('TotPkts', 'INT_TYPE'),('TotBytes', 'INT_TYPE'),('SrcBytes', 'INT_TYPE'),('Label', 'STRING_TYPE')]), {'skip': 1}))))"
     *
     * java -cp /home/dhutchis/gits/lara-graphulo/target/lara-graphulo-1.0-SNAPSHOT-all.jar edu.washington.cs.laragraphulo.Main "FileStore('/home/dhutchis/gits/raco/raco/backends/federated/tests/V5407830105', 'CSV', {}, Apply([('src_ip', NamedAttributeRef('SrcAddr')), ('dst_ip', NamedAttributeRef('DstAddr')), ('value', NumericLiteral(1.0))], Select(GT(UnnamedAttributeRef(0, None), NumericLiteral(500)), Scan(RelationKey('public','adhoc','netflow'), Scheme([(u'TotBytes', 'INT_TYPE'), (u'StartTime', 'STRING_TYPE'), (u'SrcAddr', 'STRING_TYPE'), (u'DstAddr', 'STRING_TYPE'), (u'RATE', 'DOUBLE_TYPE'), (u'Dur', 'DOUBLE_TYPE'), (u'Dir', 'STRING_TYPE'), (u'Proto', 'STRING_TYPE'), (u'Sport', 'STRING_TYPE'), (u'Dport', 'STRING_TYPE'), (u'State', 'STRING_TYPE'), (u'sTos', 'LONG_TYPE'), (u'dTos', 'LONG_TYPE'), (u'TotPkts', 'LONG_TYPE'), (u'SrcBytes', 'LONG_TYPE'), (u'Label', 'STRING_TYPE')]), (<raco.backends.myria.catalog.MyriaCatalog object at 0x7f207134fbd0>, 7), RepresentationProperties(frozenset([UnnamedAttributeRef(0, None), UnnamedAttributeRef(1, None)]), None, None)))))"
     * ```
     *
     *
     */
    @JvmStatic
    fun main(args: Array<String>) {
      val opts = Opts()
      opts.parseArgs(Main::class.java.name, args)
      if (opts.query.isEmpty()) {
        System.err.println("Please provide a query, in the form of a Raco logical plan repr.")
        System.exit(1)
      }
      val query = opts.query.reduce { a, b -> "$a $b" }
      logger.info{"Query: $query"}

      val config: AccumuloConfig = getConfig(opts)
      logger.info{"Accumulo Config: $config"}

      val ret = doQuery(query, config)
      logger.debug{"Exit with status $ret"}
      System.exit(ret)
    }


    private fun getConfig(opts: Opts): AccumuloConfig {
      // hard-coded
      return AccumuloConfigImpl(
          instanceName = "accumulo-1.8",
          zookeeperHosts = "localhost:2181",
          username = "root",
          authenticationToken = PasswordToken("secret")
      )
    }


    private fun doQuery(query: String, config: AccumuloConfig): Int {
      val racoOpPTree = PTree.Companion.parseRaco(query)
      logger.debug{"PTree Raco: $racoOpPTree"}

      val racoOp = RacoOperator.parsePTreeToRacoTree(racoOpPTree)
      logger.debug{"Full  Raco: $racoOp"}

      val callables = executorsRacoOnAccumulo(racoOp, config)
      logger.debug{"Callables : $callables"}

      System.currentTimeMillis().let {
        executeTasksSerial(callables).mapIndexed { i, res -> logger.info{"result $i: $res"} }
        System.currentTimeMillis()-it
      }.let {
        logger.info{"time to run: $it"}
      }

      return 0
    }

  }


}



