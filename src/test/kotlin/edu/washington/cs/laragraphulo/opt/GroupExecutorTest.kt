package edu.washington.cs.laragraphulo.opt

import com.google.common.util.concurrent.Uninterruptibles
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

class GroupExecutorTest {

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

    val GetAndIncrementer = Callable<Int> {
      counter.getAndIncrement()
    }
    val parallelFutures = ge.submitParallelTasks(listOf(
        GetAndIncrementer, GetAndIncrementer, GetAndIncrementer
    ))
    val lastFuture = ge.submitTask(GetAndIncrementer)

    val plantedFailure = RuntimeException("a planted failure")
    val failedFuture = ge.submitTask(Callable { throw plantedFailure })
    val afterFailedFuture = ge.submitTask(GetAndIncrementer)

    assertEquals(0, firstFuture.get())
    assertEquals(setOf(0,1,2), parallelFutures.map { it.get() }.toSet())
    assertEquals(3, lastFuture.get())
    assertEquals(4, counter.get())

    try { println(afterFailedFuture.get()) }
    catch (e: Exception) { println("got $e") }

    // verify that no code runs after a task fails occurs
    // tasks submitted after a failure should be cancelled
    try { afterFailedFuture.get(); fail() }
    catch (e: CancellationException) { }
    assertEquals(4, counter.get())

    // the failed task itself should be an ExecutionException wrapping the original failure reason
    try { failedFuture.get() }
    catch (e: ExecutionException) { assertEquals(plantedFailure, e.cause) }
    assertEquals(4, counter.get())

    // attempts to submit a task after a failure should throw an exception
    try { ge.submitTask(GetAndIncrementer); fail() }
    catch (e: RejectedExecutionException) { }
  }

}
