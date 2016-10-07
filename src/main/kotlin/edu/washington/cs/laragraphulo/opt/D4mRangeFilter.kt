package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableRangeSet
import com.google.common.collect.RangeSet
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.Filter
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.OptionDescriber
import org.apache.accumulo.core.iterators.SortedKeyValueIterator

import java.io.IOException
import java.util.Arrays
import java.util.Collections
import java.util.SortedSet
import java.util.TreeSet

/**
 * Only emit entries whose column appears in the given collection of ranges.
 */
class D4mRangeFilter : Filter() {

  enum class KeyPart{
    ROW, COLF, COLQ, VAL;
  }

  private var keyPart = KeyPart.ROW
  private var filter: RangeSet<ByteSequence> = ImmutableRangeSet.of(com.google.common.collect.Range.all<ByteSequence>())

  override fun accept(key: Key, value: Value): Boolean {
    val bs = getKeyBytes(key, value, keyPart)
    return filter.contains(bs)
  }

  @Throws(IOException::class)
  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment?) {
    super.init(source, options, env)
    if (options.containsKey(FILTER))
      filter = GraphuloUtil.d4mRowToGuavaRangeSet(options[FILTER], false)
    if (options.containsKey(KEYPART))
      keyPart = KeyPart.valueOf(options[KEYPART]!!)
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    val io = super.describeOptions()
    io.setName("D4mRangeFilter")
    io.setDescription("Filter based on ranges of strings in D4M syntax")
    val values = KeyPart.values()
    val strs = arrayOfNulls<String>(values.size)
    for (i in values.indices) {
      strs[i] = values[i].name
    }
    io.addNamedOption(KEYPART, "Which part of key to filter [" + Arrays.toString(strs) + "] [default " + KeyPart.COLQ.name + "]")
    io.addNamedOption(FILTER, "Column ranges to scan for remote Accumulo table, Matlab syntax. (default ':,' all)")
    return io
  }

  override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key, Value> {
    val result = super.deepCopy(env) as D4mRangeFilter
    result.filter = filter
    result.keyPart = keyPart
    return result
  }

  companion object {

    fun getKeyBytes(k: Key, v: Value, kp: KeyPart): ByteSequence {
      when (kp) {
        KeyPart.ROW -> return k.rowData
        KeyPart.COLF -> return k.columnFamilyData
        KeyPart.COLQ -> return k.columnQualifierData
        KeyPart.VAL -> return ArrayByteSequence(v.get())
        else -> throw AssertionError()
      }
    }

    val FILTER = "filter"
    val KEYPART = "keyPart"

    fun iteratorSetting(priority: Int, keyPart: KeyPart?, filter: String): IteratorSetting {
      val itset = IteratorSetting(priority, D4mRangeFilter::class.java)
      if (keyPart != null)
        itset.addOption(KEYPART, keyPart.name)
      itset.addOption(FILTER, filter)
      return itset
    }

    fun iteratorSetting(priority: Int, keyPart: KeyPart, filter: String, negate: Boolean): IteratorSetting {
      val itset = iteratorSetting(priority, keyPart, filter)
      itset.addOption(Filter.NEGATE, java.lang.Boolean.toString(negate))
      return itset
    }
  }
}
