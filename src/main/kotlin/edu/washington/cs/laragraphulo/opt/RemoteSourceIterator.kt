package edu.washington.cs.laragraphulo.opt

import com.google.common.base.Preconditions
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import edu.washington.cs.laragraphulo.util.GraphuloUtil
import org.apache.accumulo.core.client.AccumuloException
import org.apache.accumulo.core.client.AccumuloSecurityException
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.ClientSideIteratorScanner
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.iterators.user.WholeRowIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.HashMap
import java.util.SortedSet
import java.util.TreeSet

/**
 * Reads from a remote Accumulo table.
 */
class RemoteSourceIterator : SortedKeyValueIterator<Key, Value>/*, OptionDescriber*/ {

  /** The original options passed to init. Retaining this makes deepCopy much easier-- call init again and done!  */
  private var origOptions: Map<String, String>? = null

  private var instanceName: String? = null
  private var tableName: String? = null
  private var zookeeperHost: String? = null
  private var username: String? = null
  private var auth: AuthenticationToken? = null
  private var authorizations = Authorizations.EMPTY
  /**
   * Zookeeper timeout in milliseconds
   */
  private var timeout = -1

  private var doWholeRow = false
  private var doClientSideIterators = false
  private var dynamicIteratorSetting: DynamicIteratorSetting? = null
  private var rowRanges: SortedSet<Range> = TreeSet(setOf(Range()))
  /**
   * The range given by seek. Clip to this range.
   */
  private var seekRange: Range? = null

  /**
   * Holds the current range we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private var rowRangeIterator: Iterator<Range>? = null
  private var colFilter = ""

  /**
   * Created in init().
   */
  private var scanner: Scanner? = null
  /**
   * Buffers one entry from the remote table.
   */
  private var remoteIterator: PeekingIterator<Map.Entry<Key, Value>>? = null

  private fun parseOptions(map: Map<String, String>) {
    val diterMap = HashMap<String, String>()
    var token: String? = null
    var tokenClass: String? = null
    for (optionEntry in map.entries) {
      val optionKey = optionEntry.key
      val optionValue = optionEntry.value
      if (optionValue.isEmpty())
        continue
      if (optionKey.startsWith(ITER_PREFIX)) {
        diterMap.put(optionKey.substring(ITER_PREFIX.length), optionValue)
      } else {
        when (optionKey) {
          ZOOKEEPERHOST -> zookeeperHost = optionValue
          TIMEOUT -> timeout = Integer.parseInt(optionValue)
          INSTANCENAME -> instanceName = optionValue
          TABLENAME -> tableName = optionValue
          USERNAME -> username = optionValue

          PASSWORD -> auth = PasswordToken(optionValue)
          AUTHENTICATION_TOKEN -> token = optionValue
          AUTHENTICATION_TOKEN_CLASS -> tokenClass = optionValue

          AUTHORIZATIONS // passed value must be from Authorizations.serialize()
          -> authorizations = Authorizations(optionValue.toByteArray(StandardCharsets.UTF_8))
          "doWholeRow" -> doWholeRow = java.lang.Boolean.parseBoolean(optionValue)
          ROWRANGES -> rowRanges = parseRanges(optionValue)
          COLFILTER -> colFilter = optionValue //GraphuloUtil.d4mRowToTexts(optionValue);
          DOCLIENTSIDEITERATORS -> doClientSideIterators = java.lang.Boolean.parseBoolean(optionValue)
          else -> log.warn("Unrecognized option: " + optionEntry)
        }
      }
      //      log.trace("Option OK: " + optionEntry);
    }
    require(auth == null && token != null && tokenClass != null || token == null && tokenClass == null && auth != null) {
      "must specify only one kind of authentication: password=$auth, token=$token, tokenClass=$tokenClass" }
    if (auth == null) {
      auth = GraphuloUtil.subclassNewInstance(tokenClass!!, AuthenticationToken::class.java)
      SerializationUtil.deserializeWritableBase64(auth!!, token!!)
    }

    if (!diterMap.isEmpty())
      dynamicIteratorSetting = DynamicIteratorSetting.fromMap(diterMap)
    // Required options
    if (zookeeperHost == null ||
        instanceName == null ||
        tableName == null ||
        username == null ||
        auth == null)
      throw IllegalArgumentException("not enough options provided")
  }

  @Throws(IOException::class)
  override fun init(source: SortedKeyValueIterator<Key, Value>?, map: Map<String, String>, iteratorEnvironment: IteratorEnvironment) {
    if (source != null)
      log.warn("RemoteSourceIterator ignores/replaces parent source passed in init(): " + source)
    origOptions = HashMap(map) // defensive copy

    parseOptions(map)
    setupConnectorScanner()

    log.debug("RemoteSourceIterator on table $tableName: init() succeeded")
  }

  private fun setupConnectorScanner() {
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

    try {
      scanner = connector.createScanner(tableName, authorizations)
    } catch (e: TableNotFoundException) {
      log.error(tableName + " does not exist in instance " + instanceName, e)
      throw RuntimeException(e)
    }

    if (doClientSideIterators)
      scanner = ClientSideIteratorScanner(scanner!!)

    if (dynamicIteratorSetting == null)
      GraphuloUtil.applyGeneralColumnFilter(colFilter, scanner!!, 10)
    else {
      GraphuloUtil.applyGeneralColumnFilter(colFilter, scanner!!, dynamicIteratorSetting!!, false) // prepend
      scanner!!.addScanIterator(dynamicIteratorSetting!!.toIteratorSetting())
    }

    if (doWholeRow) { // This is a legacy setting.
      // TODO: make priority dynamic in case 25 is taken; make name dynamic in case iterator name already exists. Or buffer here.
      scanner!!.addScanIterator(IteratorSetting(25, WholeRowIterator::class.java))
    }

  }

  @Throws(Throwable::class)
  protected fun finalize() {
    scanner!!.close()
  }

  @Throws(IOException::class)
  override fun seek(range: Range, columnFamilies: Collection<ByteSequence>, inclusive: Boolean) {
    log.debug("RemoteSourceIterator on table $tableName: seek(): $range")
    /** configure Scanner to the first entry to inject after the start of the range.
     * Range comparison: infinite start first, then inclusive start, then exclusive start
     * [org.apache.accumulo.core.data.Range.compareTo]  */
    seekRange = range
    rowRangeIterator = getFirstRangeStarting(Iterators.peekingIterator(rowRanges.iterator()), range) //rowRanges.tailSet(range).iterator();
    remoteIterator = Iterators.peekingIterator(Collections.emptyIterator())
    next()
  }

  //  /**
  //   * Restrict columns fetched to the ones given. Takes effect on next seek().
  //   *
  //   * @param columns Columns to fetch. Null or empty collection for all columns.
  //   * @throws IOException
  //   */
  //  public void setFetchColumns(Collection<IteratorSetting.Column> columns) throws IOException {
  //    scanner.clearColumns();
  //    if (columns != null)
  //      for (IteratorSetting.Column column : columns) {
  //        if (column.getColumnQualifier() == null)    // fetch all columns in this column family
  //          scanner.fetchColumnFamily(column.getColumnFamily());
  //        else
  //          scanner.fetchColumn(column.getColumnFamily(), column.getColumnQualifier());
  //      }
  //  }


  override fun hasTop(): Boolean {
    return remoteIterator!!.hasNext()
  }

  @Throws(IOException::class)
  override fun next() {
    if (rowRangeIterator == null || remoteIterator == null)
      throw IllegalStateException("next() called before seek() b/c rowRangeIterator or remoteIterator not set")
    remoteIterator!!.next() // does nothing if there is no next (i.e. hasTop()==false)
    while (!remoteIterator!!.hasNext() && rowRangeIterator!!.hasNext()) {
      var range: Range? = rowRangeIterator!!.next()
      range = range!!.clip(seekRange!!, true) // clip to the seek range
      if (range == null)
      // empty intersection - no more ranges by design
        return
      scanner!!.range = range
      remoteIterator = Iterators.peekingIterator(scanner!!.iterator())
    }
    // either no ranges left and we finished the current scan OR remoteIterator.hasNext()==true
    //    if (hasTop())
    //      log.trace(tableName + " prepared next entry " + getTopKey() + " ==> "
    //          + (doWholeRow ? WholeRowIterator.decodeRow(getTopKey(), getTopValue()) : getTopValue()));
    //    else
    //      log.trace(tableName + " hasTop() == false");
  }

  override fun getTopKey(): Key {
    return remoteIterator!!.peek().key // returns null if hasTop()==false
  }

  override fun getTopValue(): Value {
    return remoteIterator!!.peek().value
  }

  override fun deepCopy(iteratorEnvironment: IteratorEnvironment): RemoteSourceIterator {
    val copy = RemoteSourceIterator()
    try {
      copy.init(null, origOptions!!, iteratorEnvironment)
    } catch (e: IOException) {
      log.error("Problem creating deepCopy of RemoteSourceIterator on table " + tableName!!, e)
      throw RuntimeException(e)
    }

    return copy
  }

  companion object {
    private val log = LogManager.getLogger(RemoteSourceIterator::class.java)

    val ZOOKEEPERHOST = "zookeeperHost"
    val TIMEOUT = "timeout"
    val INSTANCENAME = "instanceName"
    val TABLENAME = "tableName"
    val USERNAME = "username"

    val PASSWORD = "password" // alternative to AUTHENTICATION_TOKEN that passes password directly
    val AUTHENTICATION_TOKEN = "authenticationToken"
    // base64 encoding of token
    val AUTHENTICATION_TOKEN_CLASS = "authenticationTokenClass"
    // class of token
    val AUTHORIZATIONS = "authorizations"
    val ROWRANGES = "rowRanges"
    val COLFILTER = "colFilter"
    val DOCLIENTSIDEITERATORS = "doClientSideIterators"
    val ITER_PREFIX = "diter."

    fun iteratorSetting(
        priority: Int, zookeeperHost: String, timeout: Int, instanceName: String, tableName: String, username: String,
        password: String,
        authorizations: Authorizations, rowRanges: String, colFilter: String, doClientSideIterators: Boolean,
        remoteIterators: DynamicIteratorSetting): IteratorSetting {
      Preconditions.checkNotNull(tableName, "Param %s is required", TABLENAME)
      return IteratorSetting(priority, RemoteSourceIterator::class.java, optionMap(null, tableName, zookeeperHost, timeout, instanceName,
          username, password, authorizations, rowRanges, colFilter, doClientSideIterators, remoteIterators))
    }

    fun iteratorSetting(
        priority: Int, zookeeperHost: String, timeout: Int, instanceName: String, tableName: String, username: String,
        token: AuthenticationToken,
        authorizations: Authorizations, rowRanges: String, colFilter: String, doClientSideIterators: Boolean,
        remoteIterators: DynamicIteratorSetting): IteratorSetting {
      Preconditions.checkNotNull(tableName, "Param %s is required", TABLENAME)
      return IteratorSetting(priority, RemoteSourceIterator::class.java, optionMap(null, tableName, zookeeperHost, timeout, instanceName,
          username, token, authorizations, rowRanges, colFilter, doClientSideIterators, remoteIterators))
    }

    /**

     * @param map Map to reuse. Pass null to create a new HashMap.
     * *
     * @param tableName Name of table to read from.
     * *
     * @param zookeeperHost Zookeeper host for Connector to remote table.
     * *
     * @param timeout Timeout for Connector to remote table. <= 0 means use default timeout.
     * *
     * @param instanceName Instance name for Connector to remote table.
     * *
     * @param username Accumulo Username for Connector to remote table.
     * *
     * @param password Used in a PasswordToken for Connector to remote table. Passed in plaintext.
     * *
     * @param authorizations Authorizations to use while scanning. Null means [Authorizations.EMPTY]
     * *
     * @param rowRanges Applied to this class's Scanner. Null means all rows. TODO: Would using a SeekFilterIterator lead to better performance?
     * *
     * @param colFilter Column filter, see [GraphuloUtil.applyGeneralColumnFilter].
     * *
     * @param doClientSideIterators Whether to apply remoteIterators at the remote table or locally. Meaningless if remoteIterators is null. Null means false.
     * *
     * @param remoteIterators If doClientSideIterators=false, these iterators are applied on the remote server.
     * *                        If doClientSideIterators=true, these iterators are applied locally.
     * *                        Null means no extra iterators.
     * *
     * @return map with options filled in.
     */
    fun optionMap(
        map: MutableMap<String, String>?, tableName: String?, zookeeperHost: String?, timeout: Int, instanceName: String?, username: String?,
        password: String?,
        authorizations: Authorizations?, rowRanges: String?, colFilter: String?, doClientSideIterators: Boolean?,
        remoteIterators: DynamicIteratorSetting?): MutableMap<String, String> {
      var map = map
      if (map == null)
        map = HashMap()
      if (tableName != null)
        map.put(TABLENAME, tableName)
      if (zookeeperHost != null)
        map.put(ZOOKEEPERHOST, zookeeperHost)
      if (timeout > 0)
        map.put(TIMEOUT, Integer.toString(timeout))
      if (instanceName != null)
        map.put(INSTANCENAME, instanceName)
      if (username != null)
        map.put(USERNAME, username)
      if (password != null)
        map.put(PASSWORD, password)
      if (authorizations != null && authorizations != Authorizations.EMPTY)
        map.put(AUTHORIZATIONS, authorizations.serialize())
      if (rowRanges != null)
        map.put(ROWRANGES, rowRanges)
      if (colFilter != null)
        map.put(COLFILTER, colFilter)
      if (doClientSideIterators != null)
        map.put(DOCLIENTSIDEITERATORS, doClientSideIterators.toString())
      if (remoteIterators != null)
        map.putAll(remoteIterators.buildSettingMap(ITER_PREFIX))
      return map
    }

    /**

     * @param map Map to reuse. Pass null to create a new HashMap.
     * *
     * @param tableName Name of table to read from.
     * *
     * @param zookeeperHost Zookeeper host for Connector to remote table.
     * *
     * @param timeout Timeout for Connector to remote table. <= 0 means use default timeout.
     * *
     * @param instanceName Instance name for Connector to remote table.
     * *
     * @param username Accumulo Username for Connector to remote table.
     * *
     * @param token AuthenticationToken containing credentials for user. Passed via Base64 encoding (no encryption).
     * *
     * @param authorizations Authorizations to use while scanning. Null means [Authorizations.EMPTY]
     * *
     * @param rowRanges Applied to this class's Scanner. Null means all rows. TODO: Would using a SeekFilterIterator lead to better performance?
     * *
     * @param colFilter Column filter, see [GraphuloUtil.applyGeneralColumnFilter].
     * *
     * @param doClientSideIterators Whether to apply remoteIterators at the remote table or locally. Meaningless if remoteIterators is null. Null means false.
     * *
     * @param remoteIterators If doClientSideIterators=false, these iterators are applied on the remote server.
     * *                        If doClientSideIterators=true, these iterators are applied locally.
     * *                        Null means no extra iterators.
     * *
     * @return map with options filled in.
     */
    fun optionMap(
        map: MutableMap<String, String>?, tableName: String?, zookeeperHost: String?, timeout: Int, instanceName: String?, username: String?,
        token: AuthenticationToken?,
        authorizations: Authorizations?, rowRanges: String?, colFilter: String?, doClientSideIterators: Boolean?,
        remoteIterators: DynamicIteratorSetting?): Map<String, String> {
      var map = map
      if (map == null)
        map = HashMap()
      if (tableName != null)
        map.put(TABLENAME, tableName)
      if (zookeeperHost != null)
        map.put(ZOOKEEPERHOST, zookeeperHost)
      if (timeout > 0)
        map.put(TIMEOUT, Integer.toString(timeout))
      if (instanceName != null)
        map.put(INSTANCENAME, instanceName)
      if (username != null)
        map.put(USERNAME, username)
      if (token != null) {
        map.put(AUTHENTICATION_TOKEN_CLASS, token.javaClass.name)
        map.put(AUTHENTICATION_TOKEN, SerializationUtil.serializeWritableBase64(token))
      }
      if (authorizations != null && authorizations != Authorizations.EMPTY)
        map.put(AUTHORIZATIONS, authorizations.serialize())
      if (rowRanges != null)
        map.put(ROWRANGES, rowRanges)
      if (colFilter != null)
        map.put(COLFILTER, colFilter)
      if (doClientSideIterators != null)
        map.put(DOCLIENTSIDEITERATORS, doClientSideIterators.toString())
      if (remoteIterators != null)
        map.putAll(remoteIterators.buildSettingMap(ITER_PREFIX))
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
      val tmp = Range.mergeOverlapping(rngs)
      return TreeSet(tmp)
    }

    /**
     * Advance to the first subset range whose end key >= the seek start key.
     */
    fun getFirstRangeStarting(iter: PeekingIterator<Range>, seekRange: Range): Iterator<Range> {
      if (!seekRange.isInfiniteStartKey)
        while (iter.hasNext() && !iter.peek().isInfiniteStopKey
            && (iter.peek().endKey.equals(seekRange.startKey) && !seekRange.isEndKeyInclusive || iter.peek().endKey.compareTo(seekRange.startKey) < 0)) {
          iter.next()
        }
      return iter
    }
  }
}
