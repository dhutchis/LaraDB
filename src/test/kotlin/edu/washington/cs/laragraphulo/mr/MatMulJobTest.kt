package edu.washington.cs.laragraphulo.mr

import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.junit.Assert.*
import org.junit.Test

class MatMulJobTest {

  @Test
  fun testSaneMutation() {
    val m = Mutation("a")
    m.put(EMPTY, Text("b"), Value("1".toByteArray()))
    for (cu in m.updates) {
      println(String.format("column: %s:%s", hexDump(cu.columnFamily), hexDump(cu.columnQualifier)))
      println(String.format("security: %s", ColumnVisibility(cu.columnVisibility).toString()))
      println(String.format("value: %s", hexDump(cu.value)))
    }
  }

  private fun hexDump(ba: ByteArray): String {
    val sb = StringBuilder()
    for (b in ba) {
      if (b > 0x20 && b < 0x7e)
        sb.append(b.toChar())
      else
        sb.append(String.format("x%02x", b))
    }
    return sb.toString()
  }

  companion object {
    private val EMPTY = Text()
  }
}