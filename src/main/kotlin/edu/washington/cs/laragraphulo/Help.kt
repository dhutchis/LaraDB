package edu.washington.cs.laragraphulo

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.ParameterException

/**
 * Base class for command-line program options.
 */
abstract class Help {
  @Parameter(names = arrayOf("-h", "-?", "--help", "-help"), help = true)
  internal var help = false

  fun parseArgs(programName: String, args: Array<String>, vararg others: Any) {
    val commander = JCommander()
    commander.addObject(this)
    for (other in others)
      commander.addObject(other)
    commander.setProgramName(programName)
    try {
      commander.parse(*args)
    } catch (ex: ParameterException) {
      commander.usage()
      exitWithError(ex.message, 1)
    }

    if (help) {
      commander.usage()
      exit(0)
    }
  }

  fun exit(status: Int) {
    System.exit(status)
  }

  fun exitWithError(message: String?, status: Int) {
    if (message != null) System.err.println(message)
    exit(status)
  }
}
