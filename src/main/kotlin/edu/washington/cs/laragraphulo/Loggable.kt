package edu.washington.cs.laragraphulo

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Adapted from https://stackoverflow.com/questions/34416869/idiomatic-way-of-logging-in-kotlin
 *
 * Standard template:
 * ```java
companion object: Loggable {
  override val logger: Logger = logger<PUT_CLASSNAME_HERE>()
}
 * ```
 */
interface Loggable {
  val logger: Logger
}

/** Called inside a [Loggable] to setup the logger for the class. */
@Suppress("unused")
inline fun <reified T : Any> Loggable.logger(): Logger = LoggerFactory.getLogger(T::class.java)

/**
 * Lazy add a log message if isTraceEnabled is true
 */
inline fun Logger.trace(msg: () -> Any?) {
  if (this.isTraceEnabled) this.trace(msg.invoke().toString())
}


/**
 * Lazy add a log message if isDebugEnabled is true
 */
inline fun Logger.debug(msg: () -> Any?) {
  if (this.isDebugEnabled) this.debug(msg.invoke().toString())
}

/**
 * Lazy add a log message if isInfoEnabled is true
 */
inline fun Logger.info(msg: () -> Any?) {
  if (this.isInfoEnabled) this.info(msg.invoke().toString())
}

/**
 * Lazy add a log message if isWarnEnabled is true
 */
inline fun Logger.warn(msg: () -> Any?) {
  if (this.isWarnEnabled) this.warn(msg.invoke().toString())
}

/**
 * Lazy add a log message if isErrorEnabled is true
 */
inline fun Logger.error(msg: () -> Any?) {
  if (this.isErrorEnabled) this.error(msg.invoke().toString())
}

/**
 * Lazy add a log message with throwable payload if isTraceEnabled is true
 */
inline fun Logger.trace(t: Throwable, msg: () -> Any?) {
  if (this.isTraceEnabled) this.trace(msg.invoke().toString(), t)
}

/**
 * Lazy add a log message with throwable payload if isDebugEnabled is true
 */
inline fun Logger.debug(t: Throwable, msg: () -> Any?) {
  if (this.isDebugEnabled) this.debug(msg.invoke().toString(), t)
}

/**
 * Lazy add a log message with throwable payload if isInfoEnabled is true
 */
inline fun Logger.info(t: Throwable, msg: () -> Any?) {
  if (this.isInfoEnabled) this.info(msg.invoke().toString(), t)
}

/**
 * Lazy add a log message with throwable payload if isWarnEnabled is true
 */
inline fun Logger.warn(t: Throwable, msg: () -> Any?) {
  if (this.isWarnEnabled) this.warn(msg.invoke().toString(), t)
}

/**
 * Lazy add a log message with throwable payload if isErrorEnabled is true
 */
inline fun Logger.error(t: Throwable, msg: () -> Any?) {
  if (this.isErrorEnabled) this.error(msg.invoke().toString(), t)
}

