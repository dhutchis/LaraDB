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

class Main {

  // maybe later adapt this from ShellOptionsJC in Accumulo's shell
  private class Opts : Help() {
//    @Parameter(names = arrayOf("-oTsampleSeqRaw"))
//    var oTsampleSeqRaw = "oTsampleSeqRaw"

    @Parameter(description = "the query to execute")
    var query: List<String> = listOf()


  }


  companion object : Loggable {
    override val logger: Logger = logger<Main>()


    /**
     * `java -jar XXX edu.washington.cs.laragraphulo.Main QUERY`
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
      logger.info{"Exit with status $ret"}
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
      logger.info{"PTree Raco: $racoOpPTree"}

      val racoOp = RacoOperator.parsePTreeToRacoTree(racoOpPTree)
      logger.info{"Full  Raco: $racoOp"}

      val callables = executorsRacoOnAccumulo(racoOp, config)
      logger.info{"Callables : $callables"}

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



