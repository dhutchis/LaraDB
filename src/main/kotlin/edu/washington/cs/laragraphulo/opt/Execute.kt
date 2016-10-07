package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.opt.raco.PTree
import edu.washington.cs.laragraphulo.opt.raco.RacoOperator
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import java.util.concurrent.Callable


val hardcodedAccumuloConfig = AccumuloConfig(
    instanceName = "instance",
    zookeeperHosts = "localhost:2181",
    username = "root",
    authenticationToken = PasswordToken("secret")
)

const val hardcodedDefaultScanTable = "RunnerTable"


fun executorsRacoOnAccumulo(
    racoOp: RacoOperator,
    accumuloConfig: AccumuloConfig
): List<Callable<*>> {
  val (op, sap, scanTable, tasksBefore, tasksAfter) = racoToAccumulo(racoOp, accumuloConfig)
  @Suppress("UNCHECKED_CAST")
  (op as Op<SKVI>)

  val tasks: List<Callable<*>>
  if (scanTable == null) {
    tasks = tasksBefore + listOf(CreateTableTask(hardcodedDefaultScanTable, accumuloConfig),
        skviOpToTask(op, accumuloConfig, hardcodedDefaultScanTable)) + tasksAfter
  } else {
    tasks = tasksBefore + skviOpToTask(op, accumuloConfig, scanTable) + tasksAfter
  }

  return tasks
}

fun executeTasksSerial(tasks: List<Callable<*>>): List<*> {
  return tasks.map { it.call() }
}


fun main(args: Array<String>) {
  require(args.size != 1) { "Please pass a raco op string as input." }

  val racoOpStr = args[0]
  println("Input Raco: $racoOpStr")
  val racoOpPTree = PTree.parseRaco(racoOpStr)
  println("PTree Raco: $racoOpPTree")
  val racoOp = RacoOperator.parsePTreeToRacoTree(racoOpPTree)
  println("Full  Raco: $racoOp")
  val callables = executorsRacoOnAccumulo(racoOp, hardcodedAccumuloConfig)
  println("Callables : $callables")
  System.currentTimeMillis().let { executeTasksSerial(callables).mapIndexed { i, res -> println("result $i: $res") }
    System.currentTimeMillis()-it }.let { println("time to run: $it") }
}

