package edu.washington.cs.laragraphulo.opt


import com.google.common.base.Preconditions
import com.google.common.base.Strings
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.client.AccumuloException
import org.apache.accumulo.core.client.AccumuloSecurityException
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.MultiTableBatchWriter
import org.apache.accumulo.core.client.MutationsRejectedException
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.PartialKey
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.OptionDescriber
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Arrays
import java.util.Collections
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.SortedSet
import java.util.TreeSet
import java.util.concurrent.atomic.AtomicInteger

import java.nio.charset.StandardCharsets.UTF_8

/**
 * SKVI that writes to an Accumulo table.
 * Does all work in seek() method. hasTop() is always false.
 *
 *
 * Do not seek this iterator with the column visibility set.
 * It may confuse the iterator into thinking it has already processed some entries.
 */
class RemoteWriteIterator : OptionDescriber, SortedKeyValueIterator<Key, Value> {
  private val thisInst: Int

  init {
    thisInst = instCnt.andIncrement
  }

  /** The original options passed to init. Retaining this makes deepCopy much easier-- call init again and done!  */
  private lateinit var origOptions: Map<String, String>
  private var numRejects = 0

  private var source: SortedKeyValueIterator<Key, Value>? = null
  private var instanceName: String? = null
  private var tableName: String? = null
  private var tableNameTranspose: String? = null
  private var zookeeperHost: String? = null
  private var username: String? = null
  private var auth: AuthenticationToken? = null
  /**
   * Zookeeper timeout in milliseconds
   */
  private var timeout = -1
  private var numEntriesCheckpoint = -1
  /** Reduce-like functionality.  */
  private var reducer: Reducer = NOOP_REDUCER()

  internal class NOOP_REDUCER : Reducer {
    override fun init(options: Map<String, String>, env: IteratorEnvironment) {}
    override fun reset() {}
    override fun update(k: Key, v: Value) {}
    override fun combine(another: ByteArray) {}
    override fun hasTopForClient(): Boolean = false
    override val forClient: ByteArray? = null
  }

  private val reducerOptions = HashMap<String, String>()

  /**
   * Created in init().
   */
  private var writerAll: MultiTableBatchWriter? = null
  private var writer: BatchWriter? = null
  private var writerTranspose: BatchWriter? = null
  /**
   * # entries written so far. Reset to 0 at writeUntilSave().
   */
  private var entriesWritten: Int = 0
  /**
   * The last key returned by the source's safe state, or the seekRange's start key.
   */
  private var lastSafeKey = Key()

  /**
   * The r given by seek. Clip to this r.
   */
  private var seekRange: Range? = null
  /**
   * Set in options.
   */
  private val rowRanges = RangeSet()
  /**
   * The String-length width of the size of rowRanges.
   * An upper bound used for encoding the maximum number of times to next() forward through rowRangeIterator.
   */
  private var rowRangesSizeWidth = 1
  /**
   * Holds the current r we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private var rowRangeIterator: PeekingIterator<Range>? = null
  /**
   * The number of times next() is called on rowRangeIterator.
   * Used for recovering state if the iterator is torn down.
   */
  private var numRowRangesIterated = 0
  private var seekColumnFamilies: Collection<ByteSequence>? = null
  private var seekInclusive: Boolean = false
  /** (default 25, limited by # of cores) number of threads to use for sending mutations  */
  private var batchWriterThreads = 25

  override fun describeOptions(): OptionDescriber.IteratorOptions {
    return iteratorOptions
  }

  override fun validateOptions(options: Map<String, String>): Boolean {
    return validateOptionsStatic(options)
  }

  @SuppressWarnings("unchecked")
  private fun parseOptions(map: Map<String, String>) {
    var token: String? = null
    var tokenClass: String? = null
    for (optionEntry in map.entries) {
      val optionKey = optionEntry.key
      val optionValue = optionEntry.value
      if (optionKey.startsWith(REDUCER + GraphuloUtil.OPT_SUFFIX)) {
        val keyAfterPrefix = optionKey.substring((REDUCER + GraphuloUtil.OPT_SUFFIX).length)
        reducerOptions.put(keyAfterPrefix, optionValue)
      } else {
        when (optionKey) {
          RemoteSourceIterator.ZOOKEEPERHOST -> zookeeperHost = optionValue
          RemoteSourceIterator.TIMEOUT -> timeout = Integer.parseInt(optionValue)
          RemoteSourceIterator.INSTANCENAME -> instanceName = optionValue
          RemoteSourceIterator.TABLENAME -> {
            tableName = optionValue
            if (tableName!!.isEmpty())
              tableName = null
          }
          TABLENAMETRANSPOSE -> {
            tableNameTranspose = optionValue
            if (tableNameTranspose!!.isEmpty())
              tableNameTranspose = null
          }
          RemoteSourceIterator.USERNAME -> username = optionValue
          RemoteSourceIterator.PASSWORD -> auth = PasswordToken(optionValue)
          RemoteSourceIterator.AUTHENTICATION_TOKEN -> token = optionValue
          RemoteSourceIterator.AUTHENTICATION_TOKEN_CLASS -> tokenClass = optionValue

          REDUCER -> reducer = GraphuloUtil.subclassNewInstance(optionValue, Reducer::class.java)

          NUMENTRIESCHECKPOINT -> numEntriesCheckpoint = Integer.parseInt(optionValue)

          RemoteSourceIterator.ROWRANGES -> {
            rowRanges.setTargetRanges(parseRanges(optionValue))
            rowRangesSizeWidth = Integer.toString(rowRanges.size()).length
          }

        //          case "trace":
        //            Watch.enableTrace = Boolean.parseBoolean(optionValue);
        //            break;

          OPT_BATCHWRITERTHREADS -> batchWriterThreads = Integer.parseInt(optionValue)
          else -> log.warn("Unrecognized option: " + optionEntry)
        }
      }
    }
    require(auth == null && token != null && tokenClass != null || token == null && tokenClass == null && auth != null) {
      "must specify only one kind of authentication: password=$auth, token=$token, tokenClass=$tokenClass" }
    if (auth == null) {
      auth = GraphuloUtil.subclassNewInstance(tokenClass!!, AuthenticationToken::class.java)
      SerializationUtil.deserializeWritableBase64(auth!!, token!!)
    }
    // Required options
    if (//(tableName == null && tableNameTranspose == null && reducer.getClass().equals(NOOP_REDUCER.class)) ||
    // ^ Allowing case where RWI just acts as a RowRangeFilter / column r filter effectively
    (tableName != null || tableNameTranspose != null) && (zookeeperHost == null ||
        instanceName == null ||
        username == null ||
        auth == null))
      throw IllegalArgumentException("not enough options provided")
  }

  @Throws(IOException::class)
  override fun init(source: SortedKeyValueIterator<Key, Value>?, map: Map<String, String>, iteratorEnvironment: IteratorEnvironment) {
    //    for (Map.Entry<String, String> entry : iteratorEnvironment.getConfig()) {
    //      System.out.println(entry.getKey() + " -> "+entry.getValue());
    //    }
    origOptions = HashMap(map) // defensive copy

    this.source = source
    if (source == null)
      throw IllegalArgumentException("source must be specified")

    //    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    //    watch.resetAll();
    //    System.out.println("reset watch at RemoteWriteIterator init");

    parseOptions(map)

    numEntriesCheckpoint = -3 // disable always
//    if (source !is SaveStateIterator) {
//      numEntriesCheckpoint = -2 // disable save state
//    }

    reducer.init(reducerOptions, iteratorEnvironment)

    setupConnectorWriter()

    //    log.debug("RemoteWriteIterator on table " + tableName + ": init() succeeded");
  }

  private fun setupConnectorWriter() {
    if (tableName == null && tableNameTranspose == null)
      return

    var cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost)
    if (timeout != -1)
      cc = cc.withZkTimeout(timeout)
    val instance = ZooKeeperInstance(cc)
    val connector: Connector
    try {
      connector = instance.getConnector(username, auth)
    } catch (e: AccumuloException) {
      log.error("failed to connect to Accumulo instance " + instanceName!!, e)
      throw RuntimeException(e)
    } catch (e: AccumuloSecurityException) {
      log.error("failed to connect to Accumulo instance " + instanceName!!, e)
      throw RuntimeException(e)
    }

    val bwc = BatchWriterConfig()
    bwc.maxWriteThreads = batchWriterThreads  // lower reduces variance / increase stability - higher has potentially better performance depending on your machines
    // consider max memory, max latency, timeout, ... on writer

    if (tableName != null && tableNameTranspose != null)
      writerAll = connector.createMultiTableBatchWriter(bwc)
    else
      writerAll = null

    try {
      if (tableName != null)
        writer = if (writerAll == null) connector.createBatchWriter(tableName, bwc) else writerAll!!.getBatchWriter(tableName)
      if (tableNameTranspose != null)
        writerTranspose = if (writerAll == null) connector.createBatchWriter(tableNameTranspose, bwc) else writerAll!!.getBatchWriter(tableNameTranspose)
    } catch (e: TableNotFoundException) {
      log.error("$tableName or $tableNameTranspose does not exist in instance $instanceName", e)
      throw RuntimeException(e)
    } catch (e: AccumuloSecurityException) {
      log.error("problem creating BatchWriters for $tableName and $tableNameTranspose")
      throw RuntimeException(e)
    } catch (e: AccumuloException) {
      log.error("problem creating BatchWriters for $tableName and $tableNameTranspose")
      throw RuntimeException(e)
    }

  }

  @Throws(Throwable::class)
  protected fun finalize() {
    log.info("finalize() RemoteWriteIterator " + tableName!!)
    //    System.out.println("finalize() RemoteWriteIterator " + tableName);
    if (writerAll != null)
      writerAll!!.close()
    else {
      if (writer != null)
        writer!!.close()
      if (writerTranspose != null)
        writerTranspose!!.close()
    }
  }

  @Throws(IOException::class)
  override fun seek(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {
    log.debug("RemoteWrite on table $tableName / $tableNameTranspose seek(): $range")
    //System.out.println("RW passed seek " + r + "(thread " + Thread.currentThread().getName() + ")");

    reducer.reset()
    //    boolean initialSeek = seekRange == null;
    seekRange = range
    seekColumnFamilies = columnFamilies
    seekInclusive = inclusive
    rowRangeIterator = rowRanges.iteratorWithRangeMask(seekRange!!)
    numRowRangesIterated = 0
    //    System.out.print(thisInst+" seek(): lastSafeKey=="+lastSafeKey+" rowRangeIterator:: ");
    //    PeekingIterator1<Range> ti = rowRanges.iteratorWithRangeMask(seekRange);
    //    while (ti.hasNext())
    //      System.out.print(ti.next()+" -- ");
    //    System.out.println();

    // Detect whether we are recovering from an iterator tear-down (due to SourceSwitchingIterator)
    val rangeStartVis = if (range.isInfiniteStartKey) ByteArray(0) else range.startKey.columnVisibilityData.toArray()
    var numToSkip = 0
    if (rangeStartVis.size > 0) {
      try {
        numToSkip = Integer.parseInt(String(rangeStartVis, UTF_8))
        if (numToSkip > 0)
          log.info("Detected Iterator Recovery! Skipping $numToSkip ranges.")
      } catch (ignored: NumberFormatException) {
      }

    }
    numRowRangesIterated = numToSkip
    while (numToSkip-- > 0 && rowRangeIterator!!.hasNext())
      rowRangeIterator!!.next()

    writeWrapper(true/*, initialSeek*/)
  }

  @Throws(IOException::class)
  private fun writeWrapper(doSeekNext: Boolean/*, boolean initialSeek*/): Boolean {
    var doSeekNext = doSeekNext
    var stoppedAtSafe = false
    entriesWritten = 0
    try {
      // while we have more ranges to seek
      // seek source to the next one and writeUntilSafeOrFinish()
      while (rowRangeIterator!!.hasNext()) {
        if (doSeekNext) {
          val thisTargetRange = rowRangeIterator!!.peek()
          assert(thisTargetRange.clip(seekRange, true) != null) {"problem with RangeSet iterator intersecting seekRange"}
          if (thisTargetRange.getStartKey() != null && thisTargetRange.getStartKey().compareTo(lastSafeKey) > 0) {
            // enforce timestamp == numRowRangesIterated
            val sk = thisTargetRange.getStartKey()
            lastSafeKey = Key(sk.getRow(), sk.getColumnFamily(), sk.getColumnQualifier(),
                Text(Strings.padStart(Integer.toString(numRowRangesIterated), rowRangesSizeWidth, '0').toByteArray()))
          }
          //          System.out.println(thisInst+" changing lastSafeKey to: "+lastSafeKey);
          log.debug("RemoteWrite actual seek " + thisTargetRange)// + "(thread " + Thread.currentThread().getName() + ")");
          // We could use the 10x next() heuristic here...
          //          if (!initialSeek)
          //          seekNextHeuristic(thisTargetRange);
          source!!.seek(thisTargetRange, seekColumnFamilies, seekInclusive)
        }
        doSeekNext = true
        stoppedAtSafe = writeUntilSafeOrFinish()
        if (stoppedAtSafe)
          break
        rowRangeIterator!!.next()
        numRowRangesIterated++
        lastSafeKey = Key(lastSafeKey.row, lastSafeKey.columnFamily, lastSafeKey.columnQualifier,
            Text(Strings.padStart(Integer.toString(numRowRangesIterated), rowRangesSizeWidth, '0').toByteArray()))
      }
    } finally {
      // send reducer entries, if any present
      // flush anything written
      if (entriesWritten > 0) {
        //        Watch<Watch.PerfSpan> watch = Watch.getInstance();
        //        watch.start(Watch.PerfSpan.WriteFlush);
        try {
          if (writerAll != null)
            writerAll!!.flush()
          else {
            if (writer != null)
              writer!!.flush()
            if (writerTranspose != null)
              writerTranspose!!.flush()
          }
        } catch (e: MutationsRejectedException) {
          log.warn("ignoring rejected mutations; ", e)
        }

        //        finally {
        //          watch.stop(Watch.PerfSpan.WriteFlush);
        //          watch.print();
        //        }
      }
    }
    //    System.out.println(thisInst+" finish writeWrapper with: "+entriesWritten);
    return stoppedAtSafe
  }

  /**
   * Return true if we stopped at a safe state with more entries to write, or
   * return false if no more entries to write (even if stopped at a safe state).
   */
  @Throws(IOException::class)
  private fun writeUntilSafeOrFinish(): Boolean {
    var m: Mutation
    //    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    while (source!!.hasTop()) {
      val k = source!!.topKey
      val v = source!!.topValue

      reducer.update(k, v)

      if (writer != null) {
        m = Mutation(k.rowData.toArray())
        m.put(k.columnFamilyData.toArray(), k.columnQualifierData.toArray(),
            k.columnVisibilityParsed, k.timestamp, v.get()) // no ts? System.currentTimeMillis()
        //        watch.start(Watch.PerfSpan.WriteAddMut);
        try {
          writer!!.addMutation(m)
        } catch (e: MutationsRejectedException) {
          numRejects++
          log.warn("rejected mutations #$numRejects; last one added is $m", e)
        }

        //        finally {
        //          watch.stop(Watch.PerfSpan.WriteAddMut);
        //        }
      }
      if (writerTranspose != null) {
        m = Mutation(k.columnQualifierData.toArray())
        m.put(k.columnFamilyData.toArray(), k.rowData.toArray(),
            k.columnVisibilityParsed, k.timestamp, v.get()) // no ts? System.currentTimeMillis()
        //        watch.start(Watch.PerfSpan.WriteAddMut);
        try {
          writerTranspose!!.addMutation(m)
        } catch (e: MutationsRejectedException) {
          numRejects++
          log.warn("rejected mutations #$numRejects; last one added is $m", e)
        }

        //        finally {
        //          watch.stop(Watch.PerfSpan.WriteAddMut);
        //        }
      }

      if (numRejects >= REJECT_FAILURE_THRESHOLD) { // declare global failure after 10 rejects
        // last entry emitted declares failure
        rowRangeIterator = Iterators.peekingIterator(Collections.emptyIterator())
        numRowRangesIterated = rowRanges.size()
        reducer = NOOP_REDUCER()
        return true
      }

      entriesWritten++
      // check to see if we can save state
      if (numEntriesCheckpoint > 0 && entriesWritten >= numEntriesCheckpoint) {
        throw UnsupportedOperationException("disabled numEntriesCheckpoint")
//        val safeKey = (source as SaveStateIterator).safeState()
//        if (safeKey != null) {
//          lastSafeKey = Key(safeKey)
//          // we can re-sync from the safe key; no need to encode the following
//          //          lastSafeKey = new Key(safeKey.getRow(), safeKey.getColumnFamily(), safeKey.getColumnQualifier(),
//          //              new Text(StringUtils.leftPad(Integer.toString(numRowRangesIterated), rowRangesSizeWidth, '0').getBytes(UTF_8)));
//          return true
//        }
      }

      //      watch.start(Watch.PerfSpan.WriteGetNext);
      //      try {
      source!!.next()
      //      } finally {
      //        watch.stop(Watch.PerfSpan.WriteGetNext);
      //      }
    }
    return false
  }


  override fun hasTop(): Boolean {
    //    System.out.println(thisInst+" hasTop(): entriesWritten=="+entriesWritten+" rowRangeIterator.hasNext()=="+rowRangeIterator.hasNext());
    return numRejects != -1 && (numRejects >= REJECT_FAILURE_THRESHOLD ||
        rowRangeIterator!!.hasNext() ||
        entriesWritten > 0 ||
        //source.hasTop() ||
        reducer.hasTopForClient())
  }

  @Throws(IOException::class)
  override fun next() {
    println("$thisInst next(): source.hasTop()==${source!!.hasTop()} entriesWritten==$entriesWritten rowRangeIterator.hasNext()==${rowRangeIterator!!.hasNext()}")
    if (numRejects >= REJECT_FAILURE_THRESHOLD)
      numRejects = -1
    reducer.reset()
    if (entriesWritten > 0 || rowRangeIterator!!.hasNext()) {
      if (source!!.hasTop()) {
        //        Watch<Watch.PerfSpan> watch = Watch.getInstance();
        //        watch.start(Watch.PerfSpan.WriteGetNext);
        //        try {
        source!!.next()
        //        } finally {
        //          watch.stop(Watch.PerfSpan.WriteGetNext);
        //        }
        writeWrapper(false)
      } else {
        rowRangeIterator!!.next()
        numRowRangesIterated++
        lastSafeKey = Key(lastSafeKey.row, lastSafeKey.columnFamily, lastSafeKey.columnQualifier,
            Text(Strings.padStart(Integer.toString(numRowRangesIterated), rowRangesSizeWidth, '0').toByteArray()))
        writeWrapper(true)
      }
    }
  }

  override fun getTopKey(): Key {
    //    System.out.println(thisInst+" getTopKey(): source.hasTop()=="+source.hasTop()+" lastSafeKey=="+lastSafeKey);
    if (source!!.hasTop())
      return lastSafeKey
    else {
      // maintain col vis
      val sk = lastSafeKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL)
      return Key(sk.row, sk.columnFamily, sk.columnQualifier,
          lastSafeKey.columnVisibility)
    }
  }

  override fun getTopValue(): Value {
    println("$thisInst next(): source.hasTop()==${source!!.hasTop()} entriesWritten==$entriesWritten lastSafeKey==$lastSafeKey")
    if (numRejects >= REJECT_FAILURE_THRESHOLD) {
      val orig = REJECT_MESSAGE
      val bb = ByteBuffer.allocate(orig.size + 8 + 2)
      bb.putLong(entriesWritten.toLong()).putChar(',').put(orig).rewind()
      return Value(bb)
    } else {
      val orig = //((SaveStateIterator) source).safeState().getValue().get();
          if (reducer.hasTopForClient()) reducer.forClient!! else ByteArray(0)
      val bb = ByteBuffer.allocate(orig.size + 8 + 2)
      bb.putLong(entriesWritten.toLong()).putChar(',').put(orig).rewind()
      //      log.debug("topValue entriesWritten: "+entriesWritten);
      return Value(bb)
    }
  }

  @SuppressWarnings("unchecked")
  override fun deepCopy(iteratorEnvironment: IteratorEnvironment): RemoteWriteIterator {
    val copy = RemoteWriteIterator()
    try {
      copy.init(source!!.deepCopy(iteratorEnvironment), origOptions, iteratorEnvironment)
    } catch (e: IOException) {
      log.error("Problem creating deepCopy of RemoteWriteIterator on table " + tableName!!, e)
      throw RuntimeException(e)
    }

    return copy
  }

  companion object {
    private val log = LogManager.getLogger(RemoteWriteIterator::class.java)

    private val instCnt = AtomicInteger(0)

    internal val REJECT_FAILURE_THRESHOLD = 10

    private val iteratorOptions: OptionDescriber.IteratorOptions
    val TABLENAMETRANSPOSE = "tableNameTranspose"
    val NUMENTRIESCHECKPOINT = "numEntriesCheckpoint"
    val REDUCER = "reducer"
    val OPT_BATCHWRITERTHREADS = "batchWriterThreads"

    init {
      val optDesc = LinkedHashMap<String, String>()
      optDesc.put(RemoteSourceIterator.ZOOKEEPERHOST, "address and port")
      optDesc.put(RemoteSourceIterator.TIMEOUT, "Zookeeper timeout between 1000 and 300000 (default 1000)")
      optDesc.put(RemoteSourceIterator.INSTANCENAME, "")
      optDesc.put(RemoteSourceIterator.TABLENAME, "(optional) To write entries to.")
      optDesc.put(TABLENAMETRANSPOSE, "(optional) To write entries with row and column qualifier swapped.")
      optDesc.put(RemoteSourceIterator.USERNAME, "")
      optDesc.put(RemoteSourceIterator.PASSWORD, "(Anyone who can read the Accumulo table config OR the log files will see your password in plaintext.)")
      optDesc.put(NUMENTRIESCHECKPOINT, "(optional) #entries until sending back a progress monitoring entry, if the source iterator supports it")
      optDesc.put(RemoteSourceIterator.ROWRANGES, "(optional) rows to seek to")
      optDesc.put(REDUCER, "(default does nothing) reducing function")
      optDesc.put(OPT_BATCHWRITERTHREADS, "(default 25, limited by # of cores) number of threads to use for sending mutations")
      iteratorOptions = OptionDescriber.IteratorOptions("RemoteWriteIterator",
          "Write to a remote Accumulo table.",
          optDesc,
          listOf("Reducer Options (preface each with " + REDUCER + GraphuloUtil.OPT_SUFFIX + ")"))
    }

    fun validateOptionsStatic(options: Map<String, String>): Boolean {
      RemoteWriteIterator().parseOptions(options)
      return true
    }

    /**

     * @param map Map to reuse. Pass null to create a new HashMap.
     * *
     * @param tableName Table to write entries to. Null means don't write entries.
     * *
     * @param tableNameTranspose Table to write transpose of entries to. Null means don't write transpose of entries.   *
     * *
     * @param zookeeperHost See [RemoteSourceIterator.optionMap]
     * *
     * @param timeout See [RemoteSourceIterator.optionMap]
     * *
     * @param instanceName See [RemoteSourceIterator.optionMap]
     * *
     * @param username See [RemoteSourceIterator.optionMap]
     * *
     * @param password See [RemoteSourceIterator.optionMap]
     * *
     * @param rowRanges See [RemoteSourceIterator.optionMap]
     * *
     * @param reducer Reducer class to instantiate at [.init] and [Reducer.update] with every entry RemoteWriteIterator sees.
     * *
     * @param reducerOptions Options passed to Reducer's [.init].
     * *
     * @param numEntriesCheckpoint Number of entries seen by RemoteWriteIterator before it emits a monitoring entry.
     * *                             Only used if its source iterator supports monitoring through use of the [SaveStateIterator] interface.
     * *
     * @param batchWriterThreads Number of threads to use for sending mutations; <= 0 means default
     * *
     * @return map with options filled in.
     */
    fun optionMap(
        map: MutableMap<String, String>, tableName: String, tableNameTranspose: String?, zookeeperHost: String, timeout: Int, instanceName: String, username: String, password: String,
        rowRanges: String,
        reducer: Class<out Reducer>?, reducerOptions: Map<String, String>?,
        numEntriesCheckpoint: Int, batchWriterThreads: Int): Map<String, String> {
      var map = RemoteSourceIterator.optionMap(map, tableName, zookeeperHost, timeout, instanceName, username, password,
          null, rowRanges, null, null, null)
      if (tableNameTranspose != null)
        map.put(TABLENAMETRANSPOSE, tableNameTranspose)
      if (reducer != null)
        map.put(REDUCER, reducer.name)
      if (reducerOptions != null)
        for ((key, value) in reducerOptions)
          map.put(REDUCER + GraphuloUtil.OPT_SUFFIX + key, value)
      if (numEntriesCheckpoint > 0)
        map.put(NUMENTRIESCHECKPOINT, Integer.toString(numEntriesCheckpoint))
      if (batchWriterThreads > 0)
        map.put(OPT_BATCHWRITERTHREADS, Integer.toString(batchWriterThreads))
      return map
    }

    /**
     * Parse string s in the Matlab format "row1,row5,row7,:,row9,w,:,z,zz,:,"
     * Does not have to be ordered but cannot overlap.

     * @param s -
     * *
     * @return a bunch of ranges
     */
    internal fun parseRanges(s: String): SortedSet<Range> {
      val rngs = GraphuloUtil.d4mRowToRanges(s)
      return TreeSet(Range.mergeOverlapping(rngs))
    }

    internal val REJECT_MESSAGE = "Server_BatchWrite_Entries_Rejected!".toByteArray(StandardCharsets.UTF_8)

    /**
     * Use this method on entries retrieved from a scan with a RemoteWriteIterator attached.
     * Decodes the Values received into the number of entries processed by RemoteWriteIterator
     * and, if a Reducer is given, updates the Reducer with the byte[] emitted from the
     * RemoteWriteIterator's server-side Reducer's [Reducer.getForClient] method.
     * @param v Value from Scanner or BatchScanner.
     * *
     * @param reducer Calls combine() on the element inside if present. Pass null to ignore elements if any returned.
     * *
     * @return Number of entries seen by the RemoteWriteIterator
     */
    fun decodeValue(v: Value, reducer: Reducer?): Long {
      val bb = ByteBuffer.wrap(v.get())
      val numEntries = bb.long
      bb.char // ','
      if (reducer != null && bb.hasRemaining()) {
        val rest = ByteArray(bb.remaining())
        if (Arrays.equals(REJECT_MESSAGE, rest)) {
          log.error("mutations rejected at server!")
        } else {
          bb.get(rest)
          reducer!!.combine(rest)
        }
      }
      return numEntries
    }
  }


}
