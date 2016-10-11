package edu.washington.cs.laragraphulo.util

import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.MutationsRejectedException
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.client.sample.SamplerConfiguration
import org.apache.accumulo.core.conf.AccumuloConfiguration
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.junit.Assert
import org.slf4j.Logger
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.*



/**
 * Helper methods for testing.
 */
object TestUtil : Loggable {
  override val logger: Logger = logger<TestUtil>()

  fun <T> List<T>.checkSorted(comparator: Comparator<T>, msg: String = "The list is not sorted") {
    val sl = ArrayList(this)
    Collections.sort(this, comparator)
    Assert.assertEquals(msg, this, sl)
  }

  fun createTestTable(conn: Connector, tableName: String) {
    if (conn.tableOperations().exists(tableName))
      conn.tableOperations().delete(tableName)
    conn.tableOperations().create(tableName)
  }

  fun createTestTable(conn: Connector, tableName: String, splits: SortedSet<Text>?) {
    createTestTable(conn, tableName)
    if (splits != null && !splits.isEmpty())
        conn.tableOperations().addSplits(tableName, splits)
  }

  /**
   * Delete table if it exists and make it afresh.
   * Optionally insert entries into the new table.
   */
  fun createTestTable(conn: Connector, tableName: String, splits: SortedSet<Text>, entriesToIngest: Map<Key, Value>?) {
    createTestTable(conn, tableName, splits)

    if (entriesToIngest != null && !entriesToIngest.isEmpty()) {
      val config = BatchWriterConfig()
      val writer = conn.createBatchWriter(tableName, config)

      var m: Mutation? = null
      try {
        for ((k, v) in entriesToIngest) {
          m = Mutation(k.rowData.toArray())
          if (k.isDeleted)
            m.putDelete(k.columnFamilyData.toArray(), k.columnQualifierData.toArray(),
                k.columnVisibilityParsed) // no ts? System.currentTimeMillis()
          else
            m.put(k.columnFamilyData.toArray(), k.columnQualifierData.toArray(),
                k.columnVisibilityParsed, v.get()) // no ts? System.currentTimeMillis()
          writer.addMutation(m)
        }
        writer.flush()
      } catch (e: MutationsRejectedException) {
        throw RuntimeException("rejected mutations; last one added is " + m!!, e)
      } finally {
        try {
          writer.close()
        } catch (e1: MutationsRejectedException) {
          logger.error("rejected mutations; last one added is " + m!!, e1)
        }

      }
    }
  }

  /**
   * Print out the expected next to the actual output from a table
   */
  fun <V> printExpectActual(expect: Map<Key, V>, actual: Map<Key, V>) {
    val acit = actual.entries.iterator()
    val exit = expect.entries.iterator()
    while (acit.hasNext() || exit.hasNext()) {
      if (acit.hasNext() && exit.hasNext())
        System.out.printf("%-51s  %s\n", exit.next(), acit.next())
      else if (acit.hasNext())
        System.out.printf("%-51s  %s\n", "", acit.next())
      else
        System.out.printf("%s\n", exit.next())
    }
  }

  fun <K,V> Iterator<Map.Entry<K, V>>.printOut() {
    while (this.hasNext()) {
      val entry = this.next()
      println(entry.key.toString() + "  " + entry.value)
    }
  }

  //    public static Collection<Map.Entry<Key,Value>> pairsToEntries(Collection<Pair<Key,Value>> entries) {
  //        Collection<Map.Entry<Key,Value>> newset = new HashSet<>(entries.size());
  //        for (Pair<Key, Value> entry : entries) {
  //            newset.add(entry.toMapEntry());
  //        }
  //        return newset;
  //    }

  fun assertEqualDoubleMap(expect: Map<Key, Value>, actual: Map<Key, Value>) {
    for ((key, value) in actual) {
      val actualValue = java.lang.Double.parseDouble(String(value.get(), StandardCharsets.UTF_8))
      val expectValue = expect[key]?.let { java.lang.Double.parseDouble(String(it.get(), StandardCharsets.UTF_8)) } ?: 0.0
      Assert.assertEquals(expectValue, actualValue, 0.001)
    }
  }

  @Throws(TableNotFoundException::class)
  fun scanTableToMap(conn: Connector, table: String, map: MutableMap<Key, Value>) {
    val scanner = conn.createBatchScanner(table, Authorizations.EMPTY, 2)
    try {
      scanner.setRanges(setOf(Range()))
      for ((key, value) in scanner) {
        map.put(key, value)
      }
    } finally {
      scanner.close()
    }
  }

  enum class MyIteratorScope {
    MAJC_FULL, MAJC_PARTIAL, MINC, SCAN
  }

  class MockIteratorEnvironment(
      val scope: MyIteratorScope
  ) : IteratorEnvironment
  {
    @Throws(IOException::class)
    override fun reserveMapFileReader(mapFileName: String): SortedKeyValueIterator<Key, Value>? {
      return null
    }

    override fun getConfig(): AccumuloConfiguration? {
      return null
    }

    override fun getIteratorScope(): IteratorUtil.IteratorScope {
      when (scope) {
        MyIteratorScope.MAJC_FULL, MyIteratorScope.MAJC_PARTIAL -> return IteratorUtil.IteratorScope.majc
        MyIteratorScope.MINC -> return IteratorUtil.IteratorScope.minc
        MyIteratorScope.SCAN -> return IteratorUtil.IteratorScope.scan
        else -> {
          Assert.fail("unknown scope: " + scope)
          throw AssertionError()
        }
      }
    }

    override fun isFullMajorCompaction(): Boolean {
      return scope === edu.washington.cs.laragraphulo.util.TestUtil.MyIteratorScope.MAJC_FULL
    }

    override fun registerSideChannel(iter: SortedKeyValueIterator<Key, Value>) {

    }

    override fun getAuthorizations(): Authorizations? {
      return null
    }

    override fun cloneWithSamplingEnabled(): IteratorEnvironment {
      throw UnsupportedOperationException("no sampling")
    }

    override fun isSamplingEnabled(): Boolean {
      return false
    }

    override fun getSamplerConfiguration(): SamplerConfiguration? {
      return null
    }
  }


}
