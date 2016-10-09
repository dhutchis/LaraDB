package edu.washington.cs.laragraphulo.opt

import com.google.common.util.concurrent.Uninterruptibles
import org.junit.Assert.*
import org.junit.Test
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
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

    val firstFuture = ge.addTask(Callable<Int> {
      assertEquals(-1, counter.get())
      Uninterruptibles.sleepUninterruptibly(1500, TimeUnit.MILLISECONDS)
      assertEquals(-1, counter.getAndSet(0))
      0
    })

    val GetAndIncrementer = Callable<Int> {
      counter.getAndIncrement()
    }

    val parallelFutures = ge.addParallelTasks(listOf(
        GetAndIncrementer, GetAndIncrementer, GetAndIncrementer
    ))

    val lastFuture = ge.addTask(GetAndIncrementer)

    val failedFuture = ge.addTask(Callable { throw RuntimeException("expected") })

    assertEquals(0, firstFuture.get())
    assertEquals(setOf(0,1,2), parallelFutures.map { it.get() }.toSet())
    assertEquals(3, lastFuture.get())
    assertEquals(4, counter.get())

    try { failedFuture.get(); @Suppress("UNREACHABLE_CODE") fail() }
    catch (e: ExecutionException) {}
  }

}
