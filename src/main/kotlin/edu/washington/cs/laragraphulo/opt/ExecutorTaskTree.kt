package edu.washington.cs.laragraphulo.opt

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadPoolExecutor

import com.google.common.util.concurrent.AsyncFunction
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.admin.NewTableConfiguration
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.OptionDescriber
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.security.Authorizations
import java.io.IOException
import java.io.Serializable
import java.util.*

/** see [StackOverflow](http://stackoverflow.com/questions/19138212/how-to-implement-a-dag-like-scheduler-in-java) */
class ExecutorTaskTree<T> {

  private val executor = MoreExecutors.listeningDecorator(
      MoreExecutors.getExitingExecutorService(Executors.newCachedThreadPool() as ThreadPoolExecutor))
  private val futureMap = HashMap<String, ListenableFuture<T>>()

  fun addTask(name: String, callable: Callable<T>, vararg predecessorNames: String): ListenableFuture<T> {
    if (futureMap.containsKey(name))
      throw IllegalArgumentException("Task name exists: $name")

    val predecessorFutures = ArrayList<ListenableFuture<T>>()
    for (predecessorName in predecessorNames) {
      val predecessorFuture = futureMap[predecessorName] ?: throw IllegalArgumentException("Predecessor task doesn't exist: $predecessorName")
      predecessorFutures.add(predecessorFuture)
    }

    val future: ListenableFuture<T>
    if (predecessorFutures.isEmpty()) {
      future = executor.submit(callable)
    } else {
      future = Futures.transform<List<T>,T>(Futures.allAsList<T>(predecessorFutures), AsyncFunction<kotlin.collections.List<T>, T> { executor.submit(callable) }, executor)
    }
    futureMap.put(name, future)
    return future
  }

}


/**
 * Holds the information to create a [Connector], plus also the [AuthenticationToken].
 * Lazily creates a [Connector] via the [connector] property.
 */
class AccumuloConfig : Serializable {
  val authenticationToken: AuthenticationToken
  @Transient private var connectorLazy: Lazy<Connector>
  val connector: Connector
    get() = connectorLazy.value
  val instanceName: String
  val zookeeperHosts: String
  val username: String

  constructor(instanceName: String,
              zookeeperHosts: String,
              username: String,
              authenticationToken: AuthenticationToken) {
    this.authenticationToken = authenticationToken
    connectorLazy = lazy {
      val cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHosts)
      val instance = ZooKeeperInstance(cc)
      instance.getConnector(username, authenticationToken)
    }
    this.instanceName = instanceName
    this.zookeeperHosts = zookeeperHosts
    this.username = username
  }

  constructor(connector: Connector,
              authenticationToken: AuthenticationToken) {
    this.authenticationToken = authenticationToken
    connectorLazy = lazyOf(connector)
    instanceName = connector.instance.instanceName
    zookeeperHosts = connector.instance.zooKeepers
    username = connector.whoami()
  }

  @Throws(IOException::class, ClassNotFoundException::class)
  private fun readObject(`in`: java.io.ObjectInputStream) {
    `in`.defaultReadObject()

    // attempt to keep connectorLazy as a val, and set it by making it accessible
//    val clProp = AccumuloConfig::connectorLazy
//    clProp.isAccessible = true

    connectorLazy = lazy {
      val cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHosts)
      val instance = ZooKeeperInstance(cc)
      instance.getConnector(username, authenticationToken)
    }
  }


  override fun toString(): String{
    return "AccumuloConfig(instance=$instanceName)"
    // , zookeeperHosts=$zookeeperHosts, username=$username
    // authenticationToken=$authenticationToken
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as AccumuloConfig

    if (authenticationToken != other.authenticationToken) return false
    if (instanceName != other.instanceName) return false
    if (zookeeperHosts != other.zookeeperHosts) return false
    if (username != other.username) return false

    return true
  }

  override fun hashCode(): Int{
    var result = authenticationToken.hashCode()
    result = 31 * result + instanceName.hashCode()
    result = 31 * result + zookeeperHosts.hashCode()
    result = 31 * result + username.hashCode()
    return result
  }
}


class CreateTableTask(
    val tableName: String,
    val accumuloConfig: AccumuloConfig,
    val ntc: NewTableConfiguration = NewTableConfiguration()
) : Callable<Boolean> {
  /**
   * @return true if the table was created; false if it already exists
   */
  override fun call(): Boolean {
    val connector = accumuloConfig.connector
    val exists = connector.tableOperations().exists(tableName)
    if (!exists)
      connector.tableOperations().create(tableName, ntc)
    return exists
  }
}

// parameterized so that we can serialize an IteratorSetting if need be
interface Serializer<in I, out O> {
  fun serializeToString(obj: I): String
  fun deserializeFromString(str: String): O
}

data class AccumuloPipeline(
    val skvi: SKVI,
    val serializer: Serializer<SKVI,SKVI>,
    val tableName: String
)

class AccumuloPipelineTask(
    val accumuloPipeline: AccumuloPipeline,
    val accumuloConfig: AccumuloConfig
) : Callable<LinkedHashMap<Key, Value>> {

  /**
   * @return entries received from the server, gathered into memory
   */
  override fun call(): LinkedHashMap<Key, Value> {
    val connector = accumuloConfig.connector
    val bs = connector.createBatchScanner(accumuloPipeline.tableName, Authorizations.EMPTY, 15)
    val ranges = listOf(Range())
    bs.setRanges(ranges)

    val priority = 10
    // create a DynamicIterator
    val itset = DeserializeAndDelegateIterator.iteratorSetting(accumuloPipeline.serializer, accumuloPipeline.skvi, priority)
    bs.addScanIterator(itset)

    val results = LinkedHashMap<Key, Value>()
    for ((key, value) in bs) {
      results.put(key, value)
    }
    return results
  }
}

typealias SKVI = SortedKeyValueIterator<Key,Value>

class DeserializeAndDelegateIterator : SKVI, OptionDescriber {
  companion object {
    const val OPT_SERIALIZED_SKVI = "serialized_skvi"
    const val OPT_SERIALIZER_CLASS = "serializer_class"

    fun iteratorSetting(serializer: Serializer<SKVI,SKVI>, skvi: SKVI, priority: Int = 10): IteratorSetting {
      val serializer_class = skvi.javaClass
      val serialized_skvi = serializer.serializeToString(skvi)
      return IteratorSetting(priority, DeserializeAndDelegateIterator::class.java,
          mapOf(OPT_SERIALIZER_CLASS to serializer_class.name,
              OPT_SERIALIZED_SKVI to serialized_skvi))
    }

    fun deserializeFromOptions(options: Map<String,String>): SKVI {
      val serializer_class = options[OPT_SERIALIZER_CLASS] ?: throw IllegalArgumentException("no option given for $OPT_SERIALIZER_CLASS")
      val serialized_skvi = options[OPT_SERIALIZED_SKVI] ?: throw IllegalArgumentException("no option given for $OPT_SERIALIZED_SKVI")
      @Suppress("UNCHECKED_CAST")
      val serializer = GraphuloUtil.subclassNewInstance(serializer_class, Serializer::class.java) as Serializer<*,SKVI>
      return serializer.deserializeFromString(serialized_skvi)
    }
  }

  private lateinit var skvi: SKVI

  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment) {
    skvi = deserializeFromOptions(options)
    skvi.init(source, options, env)
  }
  override fun getTopValue() = skvi.topValue
  override fun next() = skvi.next()
  override fun deepCopy(env: IteratorEnvironment?) = skvi.deepCopy(env)
  override fun hasTop() = skvi.hasTop()
  override fun seek(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) = skvi.seek(range, columnFamilies, inclusive)
  override fun getTopKey() = skvi.topKey

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    return OptionDescriber.IteratorOptions("DeserializeAndDelegateIterator",
        "de-serializes an Serializer<SKVI> and delegates all SKVI operations to it",
        mapOf(OPT_SERIALIZED_SKVI to "the serialized SKVI",
            OPT_SERIALIZER_CLASS to "the class that can deserialize the skvi; must have a no-args constructor"),
        null)
  }
  override fun validateOptions(options: Map<String, String>): Boolean {
    deserializeFromOptions(options)
    return true
  }
}






