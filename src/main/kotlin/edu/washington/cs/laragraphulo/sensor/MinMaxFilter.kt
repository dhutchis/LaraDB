package edu.washington.cs.laragraphulo.sensor

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.Filter
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.OptionDescriber
import org.apache.accumulo.core.iterators.SortedKeyValueIterator

/**
 * Filter based on minimum and maximum Value (encoded as Longs).
 * Depending on [encode], either encoded as a String or by ULongLexicoder.
 */
class MinMaxFilter : Filter() {

  private var minValue = Long.MIN_VALUE
  private var maxValue = Long.MAX_VALUE
  private var encode = false
  private lateinit var origOpts: Map<String,String>

  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment?) {
    super.init(source, options, env)
    initOptions(options)
  }

  private fun initOptions(options: Map<String,String>): MinMaxFilter {
    origOpts = options
    minValue = options[MINVALUE]?.toLong() ?: minValue
    maxValue = options[MAXVALUE]?.toLong() ?: maxValue
    encode = options[ENCODE]?.toBoolean() ?: encode
    return this
  }

  override fun accept(k: Key, v: Value): Boolean {
    return v.get().toLong(encode) in minValue..maxValue
  }

  override fun deepCopy(env: IteratorEnvironment?): MinMaxFilter {
    return (super.deepCopy(env) as MinMaxFilter).initOptions(origOpts)
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    val io = super.describeOptions()
    io.setName(MinMaxFilter::class.java.canonicalName)
    io.setDescription("Filter based on Value interpreted as a Long")
    io.addNamedOption(MINVALUE, "Minimum Value, default " + minValue)
    io.addNamedOption(MAXVALUE, "Maximum Value, default " + maxValue)
    io.addNamedOption(ENCODE, "Encode with ULongLexicoder instead of String? [default false]")
    return io
  }

  override fun validateOptions(options: Map<String, String>): Boolean {
    MinMaxFilter().initOptions(options)
    return super.validateOptions(options)
  }

  companion object {
//    private val log = LogManager.getLogger(MinMaxFilter::class.java)

    val MINVALUE = "minValue"
    val MAXVALUE = "maxValue"
    val ENCODE = "encode"

    fun iteratorSetting(priority: Int, min: Long?, max: Long?, encode: Boolean = false): IteratorSetting {
      val itset = IteratorSetting(priority, MinMaxFilter::class.java)
      if (min != null) itset.addOption(MINVALUE, min.toString())
      if (max != null) itset.addOption(MAXVALUE, max.toString())
      if (encode) itset.addOption(ENCODE, encode.toString())
      return itset
    }
  }
}
