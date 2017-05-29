package edu.washington.cs.laragraphulo.examples.rainysunny

import edu.mit.ll.graphulo.apply.ApplyIterator
import edu.mit.ll.graphulo.apply.ApplyOp
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment

/** A simple iterator that changes [SUNNY] to [RAINY] in the Value of tuples seen. */
class MapRainToSunIterator : ApplyOp {
  override fun init(options: Map<String, String>?, env: IteratorEnvironment?) {}
  override fun seekApplyOp(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {}

  override fun apply(k: Key, v: Value): Iterator<Map.Entry<Key, Value>> {
    val newValue = Value(v.toString().replace("Rainy", "Sunny"))
    return mapOf(k to newValue).iterator()
  }

  companion object {
    /** A helper method that creates an [IteratorSetting], in order to use this in queries.  */
    fun iteratorSetting(priority: Int): IteratorSetting {
      val itset = IteratorSetting(priority, ApplyIterator::class.java)
      itset.addOption(ApplyIterator.APPLYOP, MapRainToSunIterator::class.java.name)
      return itset
    }
  }
}