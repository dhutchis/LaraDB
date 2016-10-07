package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.ScannerBase
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

import java.io.IOException
import java.util.ArrayList
import java.util.Deque
import java.util.EnumSet
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.LinkedList

/**
 * Solves the problem of running out of iterator priority spaces.
 * Bundles several iterators together in one.
 * @see edu.mit.ll.graphulo.skvi.DynamicIterator
 */
class DynamicIteratorSetting @JvmOverloads constructor(var diPriority: Int, diName: String?, diScopes: EnumSet<MyIteratorScope>? = EnumSet.allOf(MyIteratorScope::class.java)) {

  private val iteratorSettingList = LinkedList<IteratorSetting>()
  val diName: String
  val diScopes: EnumSet<MyIteratorScope>

  enum class MyIteratorScope {
    SCAN, MINC, MAJC_FULL, MAJC_PARTIAL;

    companion object {
      fun scopesToD4mString(scopes: EnumSet<MyIteratorScope>): String {
        val sb = StringBuilder()
        for (scope in scopes) {
          sb.append(scope.name).append(',')
        }
        return sb.toString()
      }

      fun d4mStringToScopes(s: String): EnumSet<MyIteratorScope> {
        val scopes = EnumSet.noneOf(MyIteratorScope::class.java)
        for (scope in GraphuloUtil.splitD4mString(s))
          scopes.add(MyIteratorScope.valueOf(scope.toUpperCase()))
        return scopes
      }

      fun getCoveringIteratorScope(myscopes: EnumSet<MyIteratorScope>): EnumSet<IteratorUtil.IteratorScope> {
        val scopes = EnumSet.noneOf(IteratorUtil.IteratorScope::class.java)
        for (myscope in myscopes) {
          when (myscope) {
            MAJC_FULL, MAJC_PARTIAL -> scopes.add(IteratorUtil.IteratorScope.majc)
            MINC -> scopes.add(IteratorUtil.IteratorScope.minc)
            SCAN -> scopes.add(IteratorUtil.IteratorScope.scan)
            else -> throw AssertionError()
          }
        }
        return scopes
      }
    }
  }


  init {
    Preconditions.checkArgument(diPriority > 0, "iterator priority must be >0: %s", diPriority)
    this.diName = if (diName == null || diName.isEmpty()) DynamicIterator::class.java.simpleName else diName
    this.diScopes = diScopes ?: EnumSet.allOf(MyIteratorScope::class.java)
  }

  fun prepend(setting: IteratorSetting): DynamicIteratorSetting {
    if (setting.iteratorClass == DynamicIterator::class.java.name) {
      val dis = fromMap(setting.options)
      val iterator = dis.iteratorSettingList.descendingIterator()
      while (iterator.hasNext()) {
        val itset = iterator.next()
        iteratorSettingList.addFirst(itset)
      }
    } else
      iteratorSettingList.addFirst(setting)
    return this
  }

  fun append(setting: IteratorSetting): DynamicIteratorSetting {
    if (setting.iteratorClass == DynamicIterator::class.java.name) {
      val dis = fromMap(setting.options)
      for (itset in dis.iteratorSettingList) {
        iteratorSettingList.addLast(itset)
      }
    } else
      iteratorSettingList.addLast(setting)
    return this
  }

  fun clear(): DynamicIteratorSetting {
    iteratorSettingList.clear()
    return this
  }

  /** Add the prefix to every setting option.  */
  @JvmOverloads fun buildSettingMap(pre: String? = ""): Map<String, String> {
    var pre = pre
    if (pre == null) pre = ""
    val map = HashMap<String, String>()
    map.put(pre + "0.diPriority", Integer.toString(diPriority)) // 0.diPriority -> 7
    map.put(pre + "0.diName", diName)                           // 0.diName -> DynamicIterator
    map.put(pre + "0.diScopes", MyIteratorScope!!.scopesToD4mString(diScopes))
    var prio = 1
    for (setting in iteratorSettingList) {
      val prefix = pre + prio + "." + setting.name + "."
      map.put(prefix + "class", setting.iteratorClass)        // 1.itername.class -> classname
      for ((key, value) in setting.options) {
        map.put(prefix + "opt." + key, value)  // 1.itername.opt.optkey -> optvalue
      }
      prio++
    }
    return map
  }

  fun toIteratorSetting(): IteratorSetting {
    val itset = IteratorSetting(diPriority, diName, DynamicIterator::class.java, buildSettingMap())
    // record special ON_SCOPE option that GraphuloUtil.applyIteratorSoft recognizes
    GraphuloUtil.addOnScopeOption(itset, MyIteratorScope.getCoveringIteratorScope(diScopes))
    return itset
  }

  fun addToScanner(scanner: ScannerBase) {
    if (!iteratorSettingList.isEmpty())
      scanner.addScanIterator(toIteratorSetting())
  }

  fun getIteratorSettingList(): List<IteratorSetting> {
    return ArrayList(iteratorSettingList)
  }

  override fun toString(): String {
    val sb = StringBuilder()
    for ((key, value) in buildSettingMap()) {
      sb.append(key).append(" -> ").append(value).append('\n')
    }
    return sb.toString()
  }

  /**
   * Used inside the Accumulo iterator stack to create the iterator list held in this object.
   * @see org.apache.accumulo.core.iterators.IteratorUtil.loadIterators

   * @return The iterators this object holds, loaded in order.
   */
  @SuppressWarnings("unchecked")
  @Throws(IOException::class)
  fun loadIteratorStack(source: SortedKeyValueIterator<Key, Value>, env: IteratorEnvironment): SortedKeyValueIterator<Key, Value> {
    var source = source
    //    if (log.isDebugEnabled())
    //      if (source.getClass().equals(DynamicIterator.class))
    //        log.debug("Be Careful not to reuse names! Recursive DynamicIterator: "+source);
    for (setting in iteratorSettingList) {
      val optOrig = setting.options            // the options are unmodifiable....
      val optCopy = HashMap<String, String>(optOrig.size)   // make a defensive copy so that the init function can modify them if it wants
      optCopy.putAll(optOrig)
      @Suppress("UNCHECKED_CAST")
      val tmp = GraphuloUtil.subclassNewInstance(
          setting.iteratorClass, SortedKeyValueIterator::class.java) as SortedKeyValueIterator<Key,Value>
      tmp.init(source, optCopy, env)
      source = tmp
    }
    return source
  }

  companion object {
    private val log = LogManager.getLogger(DynamicIteratorSetting::class.java)


    /** Prefix is "".
     * @see .fromMap
     */
    fun fromMap(mapOrig: Map<String, String>): DynamicIteratorSetting {
      return fromMap("", mapOrig)
    }

    /**
     * Load a DynamicIteratorSetting from a Map&lt;String,String&gt;.
     * Used inside the Accumulo iterator stack [SortedKeyValueIterator.init].
     * @param pre A prefix that must be in front of every option
     * *
     * @param mapOrig Map of options. Nothing is added or removed.
     * *
     * @return New DynamicIteratorSetting
     */
    fun fromMap(pre: String?, mapOrig: Map<String, String>): DynamicIteratorSetting {
      var pre = pre
      if (pre == null) pre = ""
      val mapCopy = LinkedHashMap(mapOrig)
      Preconditions.checkArgument(mapOrig.containsKey(pre + "0.diPriority") && mapOrig.containsKey(pre + "0.diName"), "bad map %s", mapOrig)
      val diPriotity = Integer.parseInt(mapCopy.remove(pre + "0.diPriority"))
      val diName = mapCopy.remove(pre + "0.diName")
      val diScopes = MyIteratorScope.d4mStringToScopes(mapCopy.remove(pre + "0.diScopes")!!)
      val dis = DynamicIteratorSetting(diPriotity, diName, diScopes)
      var prio = 1
      while (true) {
        val prioPrefix = "$prio."
        var clazz: String? = null
        var name: String? = null
        var clazzStr: String? = null
        var optPrefix: String? = null
        val opt = HashMap<String, String>()

        val iterator = mapCopy.entries.iterator()
        while (iterator.hasNext()) {
          val entry = iterator.next()
          var key: String = entry.key
          if (!key.startsWith(pre)) {
            iterator.remove()
            continue
          }
          key = key.substring(pre.length)

          if (name == null && key.startsWith(prioPrefix)) {
            val idxSecondDot = key.indexOf('.', prioPrefix.length)
            Preconditions.checkArgument(idxSecondDot != -1, "invalid map entry %s -> %s", key, entry.value)
            name = key.substring(prioPrefix.length, idxSecondDot)
            clazzStr = prioPrefix + name + ".class"
            optPrefix = prioPrefix + name + ".opt."
          }

          if (name != null && key == clazzStr) {
            Preconditions.checkArgument(clazz == null, "Class defined twice: %s -> %s", key, entry.value)
            clazz = entry.value
            iterator.remove()
          } else if (name != null && key.startsWith(optPrefix!!)) {
            opt.put(key.substring(optPrefix.length), entry.value)
            iterator.remove()
          }
        }
        if (name == null)
          break
        Preconditions.checkArgument(clazz != null, "no class for IteratorSetting with name %s and options %s", name, opt)
        dis.append(IteratorSetting(prio, name, clazz, opt))
        prio++
      }
      return dis
    }
  }

}
