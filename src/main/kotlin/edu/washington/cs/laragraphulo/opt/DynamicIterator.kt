package edu.washington.cs.laragraphulo.opt

import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.iterators.WrappingIterator
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

import java.io.IOException
import java.util.EnumSet

/**
 * Solves the problem of running out of iterator priority spaces.
 * Bundles several iterators together in one.
 * @see edu.mit.ll.graphulo.DynamicIteratorSetting
 */
class DynamicIterator : WrappingIterator() {

  @Throws(IOException::class)
  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment?) {
    var source = source
    val dis = DynamicIteratorSetting.fromMap(options)
    val diScopes = dis.diScopes
    val doit: Boolean
    when (env!!.iteratorScope) {
      IteratorUtil.IteratorScope.majc -> if (env.isFullMajorCompaction) {
        doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.MAJC_FULL)
        //          System.out.println("FULL MAJC "+doit);
      } else
        doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.MAJC_PARTIAL)
      IteratorUtil.IteratorScope.minc -> doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.MINC)
      IteratorUtil.IteratorScope.scan -> doit = diScopes.contains(DynamicIteratorSetting.MyIteratorScope.SCAN)
      else -> throw AssertionError()
    }
    if (doit)
      source = dis.loadIteratorStack(source, env)
    else
      log.debug("Not applying this DynamicIterator " + dis.diName + "; scope " + env.iteratorScope + " is not permitted")
    setSource(source)
  }

  override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key, Value> {
    val copy = DynamicIterator()
    copy.source = source.deepCopy(env)
    return copy
  }

  companion object {
    private val log = LogManager.getLogger(DynamicIterator::class.java)
  }
}
