package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.opt.raco.RacoOperator
import java.util.concurrent.Callable


const val hardcodedDefaultScanTable = "RunnerTable"


fun executorsRacoOnAccumulo(
    racoOp: RacoOperator,
    accumuloConfig: AccumuloConfig
): List<Callable<*>> {
  val (op, sap, scanTable, scanRange, tasksBefore, tasksAfter) = racoToAccumulo(racoOp, accumuloConfig)
  @Suppress("UNCHECKED_CAST")
  (op as Op<SKVI>)

  val tasks: List<Callable<*>>
  if (scanTable == null) {
    tasks = tasksBefore + listOf(CreateTableTask(hardcodedDefaultScanTable, accumuloConfig),
        skviOpToTask(op, accumuloConfig, hardcodedDefaultScanTable, scanRange)) + tasksAfter
  } else {
    tasks = tasksBefore + skviOpToTask(op, accumuloConfig, scanTable, scanRange) + tasksAfter
  }

  return tasks
}

fun executeTasksSerial(tasks: List<Callable<*>>): List<*> {
  return tasks.map { it.call() }
}

