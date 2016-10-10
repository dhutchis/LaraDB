package edu.washington.cs.laragraphulo.opt

import com.google.common.util.concurrent.Uninterruptibles
import mu.KLogging
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

class GroupExecutorTest {
  companion object: KLogging()

  /**
   * Tests that tasks added to a [GroupExecutor] *happen-before* tasks added afterward.
   */
  @Test
  @Suppress("UsePropertyAccessSyntax")
  fun testGroupExecutor() {
    val ge = GroupExecutor(false)
    val counter = AtomicInteger(-1)

    val firstFuture = ge.submitTask(Callable<Int> {
      assertEquals(-1, counter.get())
      Uninterruptibles.sleepUninterruptibly(1500, TimeUnit.MILLISECONDS)
      assertEquals(-1, counter.getAndSet(0))
      0
    })

    class GetAndIncrementer(val id: Int): Callable<Int> {
      override fun call(): Int {
        val c = counter.getAndIncrement(); logger.debug{"id($id) saw($c)"}; return c
      }
    }
    val parallelFutures = ge.submitParallelTasks(listOf(
        GetAndIncrementer(1), GetAndIncrementer(2), GetAndIncrementer(3)
    ))
    val lastFuture = ge.submitTask(GetAndIncrementer(4))

    val plantedFailure = RuntimeException("a planted failure")
    val failedFuture = ge.submitTask(Callable { throw plantedFailure })

    // Due to timing (if failedFuture executed quickly), the GroupExecutor may reject this one.
    // Otherwise it will be accepted and get cancelled later on.
    val afterFailedFuture = try { ge.submitTask(GetAndIncrementer(5)) }
    catch (e: RejectedExecutionException) { logger.debug("rejected"); null }

    assertEquals(0, firstFuture.get())
    assertEquals(setOf(0,1,2), parallelFutures.map { it.get() }.toSet())
    assertEquals(3, lastFuture.get())
    assertEquals(4, counter.get())

    if (afterFailedFuture != null) {
      // verify that no code runs after a task fails occurs
      // tasks submitted after a failure should be cancelled
      try { afterFailedFuture.get(); fail() }
      catch (e: CancellationException) { }
    }
    assertEquals(4, counter.get())
    assertTrue(ge.shutdown)

    // the failed task itself should be an ExecutionException wrapping the original failure reason
    try { failedFuture.get() }
    catch (e: ExecutionException) { assertEquals(plantedFailure, e.cause) }

    // attempts to submit a task after a failure should throw an exception
    try { ge.submitTask(GetAndIncrementer(6)); fail() }
    catch (e: RejectedExecutionException) { }
  }

}
