package edu.washington.cs.laragraphulo.opt

import com.google.common.annotations.GwtIncompatible
import com.google.common.base.Supplier
import com.google.common.collect.ImmutableList
import com.google.common.util.concurrent.*

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
import java.util.concurrent.*
import kotlin.reflect.jvm.javaField

/**
 * Similar to an [ExecutorService]. Executes tasks submitted in parallel groups.
 * Tasks within a group execute in parallel; tasks in separate groups execute serially.
 *
 * Each task executes in a thread named "1.1", "1.2", etc. in the first group;
 * "2.1", "2.2" in the second group; etc.
 *
 * When one task fails, all tasks halt / are cancelled, and this executor shuts down.
 * The cause of the failure is recorded in [failure].
 * It may be shut down manually before that; check [shutdown] to see if the executor is alive.
 * Attempting to add a task after the executor shuts down results in a [RejectedExecutionException].
 *
 * @param daemon If true then the threads in the pool are marked daemon and are terminated after the main application terminates.
 * */
class GroupExecutor(
    val daemon: Boolean
) {
  /** If true, then the executor cannot accept further tasks and [failure] *might* be non-null.
   * Otherwise the executor is happy and [failure] is null. */
  @Volatile var shutdown = false
    private set

  /** If this is non-null, then a failure has occurred and the GroupExecutor is shut down. */
  @Volatile var failure: Throwable? = null
    private set

  /** How many groups were submitted to this executor so far.
   * Used to name each group executed by this executor. */
  @Volatile var groupsSubmitted = 0
    private set

  /** A pool that starts with 0 threads and spins up new ones as necessary.
   * Old threads die if idle for 60s.
   * If [daemon] is true then the threads in the pool are marked daemon and are terminated after the main application terminates. */
  private val executor: ListeningExecutorService = (Executors.newCachedThreadPool() as ThreadPoolExecutor).let {
    MoreExecutors.listeningDecorator(
        if (daemon) MoreExecutors.getExitingExecutorService(it) else it)
  }

  /** Memory of the last future submitted, which has a binding to previously submitted futures,
   * if they were running at the time this was submitted. */
  @Volatile private var lastSubmittedFuture: ListenableFuture<*>? = null



  fun isTerminated(): Boolean = shutdown && executor.isTerminated

  /** [ExecutorService.shutdown] the service after all previously added tasks finished.
   * No new tasks may be added. */
  @Synchronized
  fun shutdown() {
    _shutdown(null, false)
  }

  /** See [ExecutorService.shutdownNow]. Cancels all past tasks immediately. */
  @Synchronized
  fun shutdownNow(): List<Runnable> {
    return _shutdown(null, true)!!
  }

  /** See [ExecutorService.awaitTermination]. */
  fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean {
    return executor.awaitTermination(timeout, unit)
  }


  /**
   * Run a list of tasks in parallel.
   * The tasks wait until previously submitted tasks finish. Then they are all executed in parallel.
   *
   * After calling this method, you probably want to use [Futures.addCallback]
   * to do something when the tasks complete, based on whether they succeed with a result or fail with an exception (or are cancelled).
   * Or call [Future.get] on each future to wait for them to complete.
   *
   * @return A list of futures for the tasks, in the same order as the [tasks] argument.
   * @throws RejectedExecutionException If the executor is [shutdown].
   */
  @Synchronized
  fun <T> submitParallelTasks(tasks: List<Callable<T>>): List<ListenableFuture<T>> {
    if (tasks.isEmpty()) return listOf()

    if (shutdown) {
      if (failure != null)
        throw RejectedExecutionException("This GroupExecutor is shutdown due to", failure)
      else
        throw RejectedExecutionException("The GroupExecutor was shut down previously.")
    }

    // wrap each task with a Thread rename
    groupsSubmitted++
    val thisGroupNumber = groupsSubmitted.toString()
    val ts = tasks.mapIndexed { idx, callable -> threadRenaming(callable, "GroupExecutor $thisGroupNumber.$idx") }

    val lsf = lastSubmittedFuture // safe to cache because synchronized
    @Suppress("UNCHECKED_CAST") // guaranteed by invokeAll contract
    val newsfs = if (lsf == null || lsf.isDone) {
      // this is the first task submitted
      ts.map { executor.submit(it) }
    } else {
      // For each input task, submit a task to the executor that runs
      // after the past tasks finish.
      ts.map { t ->
        Futures.transformAsync(
            lsf,
            AsyncFunction<Any?, T> { executor.submit(t) },
            executor)
      }
    }
    val tmp = Futures.allAsList<Any?>(newsfs)
    lastSubmittedFuture = tmp

    // Create a callback when allNewFuture finishes that
    // on failure, marks the executor and shuts it down
    // execute via directExecutor
    Futures.addCallback(tmp, object : FutureCallback<Any> {
      override fun onSuccess(result: Any?) { }
      override fun onFailure(t: Throwable?) {
        _shutdown(t, true)
      }
    })

    return newsfs
  }

  @Synchronized
  private fun _shutdown(t: Throwable?, now: Boolean): List<Runnable>? {
    shutdown = true
    // if a failure already occurred, no need to record another failure
    if (failure == null)
      failure = t

    return when {
      now -> {
        // canceling the last submitted future propagates back and cancels them all
        // if the last submitted future is null then nothing to cancel
        lastSubmittedFuture?.cancel(true) // mayInterruptIfRunning
        executor.shutdownNow()
      }
      lastSubmittedFuture == null -> { executor.shutdown(); null }
      else -> {
        // directExecutor
        Futures.transform(
            Futures.allAsList<Any?>(lastSubmittedFuture),
            com.google.common.base.Function<List<Any?>, Unit> { executor.shutdown() })
        null
      }
    }
  }

  /** Shortcut method to add a single task, to execute after previously submitted tasks finish.
   * See [submitParallelTasks]. */
  fun <T> submitTask(task: Callable<T>): ListenableFuture<T> = submitParallelTasks(listOf(task)).first()


  private companion object {
    /** Adapted from [Callables.threadRenaming]:
     *
     * Wraps the given callable such that for the duration of [Callable.call] the thread that is
     * running will have the given name.
     * @param callable The callable to wrap
     * @param newName New thread name
     */
    private fun <T> threadRenaming(callable: Callable<T>, newName: String): Callable<T> = Callable {
      val currentThread = Thread.currentThread()
      val oldName = currentThread.name
      val restoreName = trySetName(newName, currentThread)
      try {
        return@Callable callable.call()
      } finally {
        if (restoreName) {
          trySetName(oldName, currentThread)
        }
      }
    }

    /** From [Callables.trySetName]:
     *
     * Tries to set name of the given [Thread], returns true if successful.  */
    private fun trySetName(threadName: String, currentThread: Thread): Boolean = try {
      currentThread.name = threadName; true
    } catch (e: SecurityException) { false }
  }
}


/**
 * Holds the information to create a [Connector], plus also the [AuthenticationToken].
 * Lazily creates a [Connector] via the [connector] property.
 */
class AccumuloConfig : Serializable {
  @Transient val authenticationToken: AuthenticationToken
  private val authenticationTokenClass: Class<AuthenticationToken>

  @Transient private val connectorLazy: Lazy<Connector>
  /** Lazily constructed Connector. Constructs the Connector (thereby connecting to the Accumulo DB) when this property is referenced. */
  val connector: Connector
    get() = connectorLazy.value
  /** Whether [connector] is constructed; whether this is connected to an Accumulo DB. */
  val connected: Boolean
    get() = connectorLazy.isInitialized()

  val instanceName: String
  val zookeeperHosts: String
  val username: String

  constructor(instanceName: String,
              zookeeperHosts: String,
              username: String,
              authenticationToken: AuthenticationToken) {
    this.authenticationToken = authenticationToken
    this.authenticationTokenClass = authenticationToken.javaClass
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
    this.authenticationTokenClass = authenticationToken.javaClass
    connectorLazy = lazyOf(connector)
    instanceName = connector.instance.instanceName
    zookeeperHosts = connector.instance.zooKeepers
    username = connector.whoami()
  }

  @Throws(IOException::class)
  private fun writeObject(stream: java.io.ObjectOutputStream) {
    stream.defaultWriteObject()
    authenticationToken.write(stream)
  }

  @Throws(IOException::class, ClassNotFoundException::class)
  private fun readObject(`in`: java.io.ObjectInputStream) {
    `in`.defaultReadObject()

    val auth = GraphuloUtil.subclassNewInstance(authenticationTokenClass, AuthenticationToken::class.java)
    auth.readFields(`in`)
    // Safe to set the final field authenticationToken.
    val authField = AccumuloConfig::authenticationToken.javaField!!
    authField.isAccessible = true
    authField.set(this, auth)

    // Same approach for connectorLazy
    val clField = AccumuloConfig::connectorLazy.javaField!!
    clField.isAccessible = true
    clField.set(this, lazy {
      val cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHosts)
      val instance = ZooKeeperInstance(cc)
      instance.getConnector(username, authenticationToken)
    })
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


data class CreateTableTask(
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

data class AccumuloPipeline<D>(
    val data: D,
    val serializer: Serializer<D,D>,
    val tableName: String
)

class AccumuloPipelineTask<D>(
    val pipeline: AccumuloPipeline<D>,
    val config: AccumuloConfig,
    /** For Op<SKVI>, use [DeserializeInvokeIterator.Companion].
     * For SKVI, use [DeserializeDelegateIterator.Companion] */
    val setting: SerializerSetting<D>
) : Callable<LinkedHashMap<Key, Value>> {

  companion object {
    /** An alternative to the primary constructor for [AccumuloPipelineTask]
     * that infers the appropriate [SerializerSetting] based on whether [D]
     * is an [Op]<SKVI> or an [SKVI]. */
    inline operator fun <reified D : Any> invoke(
        pipeline: AccumuloPipeline<D>,
        config: AccumuloConfig
    ): AccumuloPipelineTask<D> {
      val jc = D::class.java

      val setting: SerializerSetting<out Any> = when {
        Op::class.java.isAssignableFrom(jc) -> DeserializeInvokeIterator.Companion
        SortedKeyValueIterator::class.java.isAssignableFrom(jc) -> DeserializeDelegateIterator.Companion
        else -> throw Exception("I don't know what iterator setting to use for $jc when invoked with pipeline $pipeline")
      }

      @Suppress("UNCHECKED_CAST")
      (setting as SerializerSetting<D>)
      return AccumuloPipelineTask(pipeline, config, setting)
    }
  }

  /**
   * @return entries received from the server, gathered into memory
   */
  override fun call(): LinkedHashMap<Key, Value> {
    val connector = config.connector
    val bs = connector.createBatchScanner(pipeline.tableName, Authorizations.EMPTY, 15)
    val ranges = listOf(Range())
    bs.setRanges(ranges)

    val priority = 10
    // create a DynamicIterator
    val itset = setting.iteratorSetting(pipeline.serializer, pipeline.data, priority)
    bs.addScanIterator(itset)

    val results = LinkedHashMap<Key, Value>()
    for ((key, value) in bs) {
      results.put(key, value)
    }
    return results
  }
}


class DeserializeInvokeIterator : DelegatingIterator(), OptionDescriber {
  companion object : SerializerSetting<Op<SKVI>>(DeserializeInvokeIterator::class.java) {

  }

  override fun initDelegate(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment): SortedKeyValueIterator<Key, Value> {
    val op = deserializeFromOptions(options)
    val skvi = op(listOf(source, options, env)) // !
    skvi.init(source, options, env)
    return skvi
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    return OptionDescriber.IteratorOptions("DeserializeAndDelegateIterator",
        "de-serializes a Serializer<Op<SKVI>>, invokes it, and delegates all SKVI operations to it",
        mapOf(SerializerSetting.OPT_SERIALIZED_DATA to "the serialized SKVI",
            SerializerSetting.OPT_SERIALIZER_CLASS to "the class that can deserialize the skvi; must have a no-args constructor"),
        null)
  }
  override fun validateOptions(options: Map<String, String>): Boolean {
    deserializeFromOptions(options)
    return true
  }
}













typealias SKVI = SortedKeyValueIterator<Key,Value>

/**
 * An iterator that delegates all methods to a delegate, computed when [init] is called.
 */
abstract class DelegatingIterator : SKVI {

  abstract fun initDelegate(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment): SKVI

  private lateinit var skvi: SKVI

  override fun init(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment) {
    skvi = initDelegate(source, options, env)
  }
  override fun getTopValue(): Value = skvi.topValue
  override fun next() = skvi.next()
  override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key,Value> = skvi.deepCopy(env)
  override fun hasTop() = skvi.hasTop()
  override fun seek(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) = skvi.seek(range, columnFamilies, inclusive)
  override fun getTopKey(): Key = skvi.topKey
}

/**
 * A template for the Companions of the Deserializing classes.
 */
abstract class SerializerSetting<D>(
    val delegatingClass: Class<out SortedKeyValueIterator<Key,Value>>
) {
  companion object {
    const val OPT_SERIALIZED_DATA = "serialized_data"
    const val OPT_SERIALIZER_CLASS = "serializer_class"
  }

  fun <T : D>iteratorSetting(serializer: Serializer<T,T>, skvi: T, priority: Int = 10): IteratorSetting {
    val serializer_class = serializer.javaClass.name
    val serialized_skvi = serializer.serializeToString(skvi)
    return IteratorSetting(priority, delegatingClass,
        mapOf(OPT_SERIALIZER_CLASS to serializer_class, OPT_SERIALIZED_DATA to serialized_skvi))
  }

  fun deserializeFromOptions(options: Map<String,String>): D {
    val serializer_class = options[OPT_SERIALIZER_CLASS] ?: throw IllegalArgumentException("no option given for $OPT_SERIALIZER_CLASS")
    val serialized_skvi = options[OPT_SERIALIZED_DATA] ?: throw IllegalArgumentException("no option given for $OPT_SERIALIZED_DATA")
    @Suppress("UNCHECKED_CAST")
    val serializer = GraphuloUtil.subclassNewInstance(serializer_class, Serializer::class.java) as Serializer<*,D>
    return serializer.deserializeFromString(serialized_skvi)
  }
}


class DeserializeDelegateIterator : DelegatingIterator(), OptionDescriber {
  companion object : SerializerSetting<SKVI>(DeserializeDelegateIterator::class.java) {

  }

  override fun initDelegate(source: SortedKeyValueIterator<Key, Value>, options: Map<String, String>, env: IteratorEnvironment): SortedKeyValueIterator<Key, Value> {
    val skvi = deserializeFromOptions(options)
    skvi.init(source, options, env)
    return skvi
  }

  override fun describeOptions(): OptionDescriber.IteratorOptions =
      OptionDescriber.IteratorOptions("DeserializeDelegateIterator",
          "de-serializes a Serializer<SKVI> and delegates all SKVI operations to it",
          mapOf(SerializerSetting.OPT_SERIALIZED_DATA to "the serialized SKVI",
              SerializerSetting.OPT_SERIALIZER_CLASS to "the class that can deserialize the skvi; must have a no-args constructor"),
          null)
  override fun validateOptions(options: Map<String, String>): Boolean {
    deserializeFromOptions(options)
    return true
  }
}







