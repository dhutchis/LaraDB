package edu.washington.cs.laragraphulo.util

import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.client.*
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.Combiner
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.LongCombiner
import org.apache.accumulo.core.iterators.user.SummingCombiner
import org.apache.hadoop.io.Text
import org.slf4j.Logger

import java.nio.charset.StandardCharsets

/**
 * Wrapper around the following tables:
 * table,
 * tableT,
 * tableDeg,
 * tableTDeg.
 * Can set the column family if you want to use a non-empty one.
 */
class D4MTableWriter
/** All values from the config object are copied.  */
(config: D4MTableWriter.D4MTableConfig) : AutoCloseable {

  override fun close() {
    this.closeIngest()
  }

  private enum class State {
    New, Open, Closed
  }

  private var state = State.New

  /** Holds configuration options to pass to constructor of D4MTableWriter.  */
  class D4MTableConfig(
      val baseName: String,
      val connector: Connector
  ) {
    var useTable = false
    var useTableT = false
    var useTableDeg = false
    var useTableDegT = false
    var useTableField = false
    var useTableFieldT = false
    var useSameDegTable = false
    var useSameFieldTable = false
    var sumTable = false
    var sumTableT = false
    var useEdgeTable = false
    var useEdgeTableT = false
    var useEdgeTableDegT = false
    var degreeUseValue = false // true = sum value in degree, false = sum 1 in degree per entry
    var colDeg = DEFAULT_DEGCOL
    var colDegT = DEFAULT_DEGCOL
    var cf = EMPTYCF
    /** The number of bytes until we flush data to the server.  */
    var batchBytes = 2000000L
        var deleteExistingTables = false

    constructor(c: D4MTableConfig) : this(c.baseName, c.connector) {
      useTable = c.useTable
      useTableT = c.useTableT
      useTableDeg = c.useTableDeg
      useTableDegT = c.useTableDegT
      useTableField = c.useTableField
      useTableFieldT = c.useTableFieldT
      useSameDegTable = c.useSameDegTable
      useSameFieldTable = c.useSameFieldTable
      colDeg = c.colDeg
      colDegT = c.colDegT
      batchBytes = c.batchBytes
      cf = c.cf
      deleteExistingTables = c.deleteExistingTables
      sumTable = c.sumTable
      sumTableT = c.sumTableT
      useEdgeTable = c.useEdgeTable
      useEdgeTableDegT = c.useEdgeTableDegT
      useEdgeTableT = c.useEdgeTableT
      degreeUseValue = c.degreeUseValue
    }
  }

  private val tconf: D4MTableConfig

  private var TNtable: String? = null
  private var TNtableT: String? = null
  private var TNtableDeg: String? = null
  private var TNtableDegT: String? = null
  private var TNtableField: String? = null
  private var TNtableFieldT: String? = null
  private var TNtableEdge: String? = null
  private var TNtableEdgeT: String? = null
  private var TNtableEdgeDegT: String? = null
  private var Btable: BatchWriter? = null
  private var BtableT: BatchWriter? = null
  private var BtableDeg: BatchWriter? = null
  private var BtableDegT: BatchWriter? = null
  private var BtableField: BatchWriter? = null
  private var BtableFieldT: BatchWriter? = null
  private var BtableEdge: BatchWriter? = null
  private var BtableEdgeT: BatchWriter? = null
  private var BtableEdgeDegT: BatchWriter? = null
  private var mtbw: MultiTableBatchWriter? = null

  init {
    tconf = D4MTableConfig(config) // no aliasing
    initBaseBames(tconf.baseName)
    openIngest()
  }

  private fun initBaseBames(baseName: String) {
    if (tconf.useTable) TNtable = baseName
    if (tconf.useTableT) TNtableT = baseName + "T"
    if (tconf.useTableDeg) TNtableDeg = baseName + "Deg"
    if (tconf.useTableDegT) TNtableDegT = if (tconf.useSameDegTable) TNtableDeg else baseName + "DegT"
    if (tconf.useTableField) TNtableField = baseName + "Field"
    if (tconf.useTableFieldT) TNtableFieldT = if (tconf.useSameFieldTable) TNtableField else baseName + "FieldT"
    if (tconf.useEdgeTable) TNtableEdge = baseName + "Edge"
    if (tconf.useEdgeTableT) TNtableEdgeT = baseName + "EdgeT"
    if (tconf.useEdgeTableDegT) TNtableEdgeDegT = baseName + "EdgeDegT"
  }


  /**
   * Create the tables to ingest to if they do not already exist.
   * Sets up iterators on degree tables if enabled.
   */
  fun createTablesSoft() {
    var btReg = false
    var btRegT = false
    var btDeg = false
    var btDegT = false
    var btField = false
    var btFieldT = false
    var btEdge = false
    var btEdgeT = false
    var btEdgeDegT = false
    if (tconf.useTable) btReg = createTableSoft(TNtable!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useTableT && TNtableT != TNtable) btRegT = createTableSoft(TNtableT!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useTableDeg) btDeg = createTableSoft(TNtableDeg!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useTableDegT && TNtableDegT != TNtableDeg) btDegT = createTableSoft(TNtableDegT!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useTableField) btField = createTableSoft(TNtableField!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useTableFieldT && TNtableFieldT != TNtableField) btFieldT = createTableSoft(TNtableFieldT!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useEdgeTable) btEdge = createTableSoft(TNtableEdge!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useEdgeTableT) btEdgeT = createTableSoft(TNtableEdgeT!!, tconf.connector, tconf.deleteExistingTables)
    if (tconf.useEdgeTableDegT) btEdgeDegT = createTableSoft(TNtableEdgeDegT!!, tconf.connector, tconf.deleteExistingTables)
    if (btReg && tconf.sumTable) assignDegreeAccumulator(TNtable!!, tconf.connector)
    if (btRegT && tconf.sumTableT) assignDegreeAccumulator(TNtableT!!, tconf.connector)
    if (btDeg) assignDegreeAccumulator(TNtableDeg!!, tconf.connector)
    if (btDegT) assignDegreeAccumulator(TNtableDegT!!, tconf.connector)
    if (btField) assignDegreeAccumulator(TNtableField!!, tconf.connector)
    if (btFieldT) assignDegreeAccumulator(TNtableFieldT!!, tconf.connector)
    if (btEdge && tconf.sumTable) assignDegreeAccumulator(TNtableEdge!!, tconf.connector)
    if (btEdgeT && tconf.sumTableT) assignDegreeAccumulator(TNtableEdgeT!!, tconf.connector)
    if (btEdgeDegT) assignDegreeAccumulator(TNtableEdgeDegT!!, tconf.connector)

  }

  fun openIngest() {
    when (state) {
      D4MTableWriter.State.New -> createTablesSoft()
      D4MTableWriter.State.Open -> throw IllegalStateException("tried to open ingest when already open")
      D4MTableWriter.State.Closed -> {
      }
    }

    val BWconfig = BatchWriterConfig()
    BWconfig.maxMemory = tconf.batchBytes
    mtbw = tconf.connector.createMultiTableBatchWriter(BWconfig)
    try {
      if (tconf.useTable) Btable = mtbw!!.getBatchWriter(TNtable)
      if (tconf.useTableT) BtableT = if (TNtableT == TNtable) Btable else mtbw!!.getBatchWriter(TNtableT)
      if (tconf.useTableDeg) BtableDeg = mtbw!!.getBatchWriter(TNtableDeg)
      if (tconf.useTableDegT) BtableDegT = if (TNtableDegT == TNtableDeg) BtableDeg else mtbw!!.getBatchWriter(TNtableDegT)
      if (tconf.useTableField) BtableField = mtbw!!.getBatchWriter(TNtableField)
      if (tconf.useTableFieldT) BtableFieldT = if (TNtableFieldT == TNtableField) BtableField else mtbw!!.getBatchWriter(TNtableFieldT)
      if (tconf.useEdgeTable) BtableEdge = mtbw!!.getBatchWriter(TNtableEdge)
      if (tconf.useEdgeTableT) BtableEdgeT = mtbw!!.getBatchWriter(TNtableEdgeT)
      if (tconf.useEdgeTableDegT) BtableEdgeDegT = mtbw!!.getBatchWriter(TNtableEdgeDegT)
    } catch (e: TableNotFoundException) {
      logger.error("crazy. Tables should have been created.", e)
    } catch (e: AccumuloSecurityException) {
      logger.warn("error creating one of the batch writers for D4MTableWriter base " + TNtable!!, e)
    } catch (e: AccumuloException) {
      logger.warn("error creating one of the batch writers for D4MTableWriter base " + TNtable!!, e)
    }

    state = State.Open
  }

  fun flushBuffers() {
    if (state != State.Open)
      throw IllegalStateException("flushing buffer when not open")
    try {
      mtbw!!.flush()
    } catch (e: MutationsRejectedException) {
      logger.warn("mutations rejected while flushing", e)
    }

  }

  /**
   * Close all enabled table batch writers.
   */
  fun closeIngest() {
    if (state != State.Open)
      throw IllegalStateException("tried to close when already closed")
    Btable = null
    BtableT = null
    BtableDeg = null
    BtableDegT = null
    BtableField = null
    BtableFieldT = null
    BtableEdge = null
    BtableEdgeT = null
    BtableEdgeDegT = null
    try {
      mtbw!!.close()
    } catch (e: MutationsRejectedException) {
      logger.warn("error closing multi table writer for D4MTableWriter", e)
    }

    state = State.Closed
  }

  @Suppress("ProtectedInFinal", "unused")
  @Throws(Throwable::class)
  protected fun finalize() {
    if (state == State.Open)
      closeIngest()
  }

  /** Ingest to all enabled tables. Use "1" for the degree table values.  */
  @JvmOverloads fun ingestRow(rowID: Text, cq: Text, v: Value = VALONE) {
    if (state != State.Open)
      openIngest()
    if (tconf.useTable) ingestRow(Btable!!, rowID, tconf.cf, cq, v)
    if (tconf.useTableT) ingestRow(BtableT!!, cq, tconf.cf, rowID, v)
    if (tconf.useTableDeg) ingestRow(BtableDeg!!, rowID, tconf.cf, tconf.colDeg, if (tconf.degreeUseValue) v else VALONE)
    if (tconf.useTableDegT) ingestRow(BtableDegT!!, cq, tconf.cf, tconf.colDegT, if (tconf.degreeUseValue) v else VALONE)
    if (tconf.useTableField) {
      val rowIDString = rowID.toString()
      val fieldSepPos: Int = rowIDString.indexOf(FIELD_SEPERATOR)
      if (fieldSepPos == -1)
        logger.warn("$TNtableField is turned on, but the row $rowIDString to ingest does not have a field seperator $FIELD_SEPERATOR")
      else {
        val rowIDField = Text(rowIDString.substring(0, fieldSepPos))
        ingestRow(BtableField!!, rowIDField, tconf.cf, tconf.colDeg, if (tconf.degreeUseValue) v else VALONE)
      }
    }
    if (tconf.useTableFieldT) {
      val cqString = cq.toString()
      val fieldSepPos: Int = cqString.indexOf(FIELD_SEPERATOR)
      if (fieldSepPos == -1)
        logger.warn("$TNtableFieldT is turned on, but the row $cqString to ingest does not have a field seperator $FIELD_SEPERATOR")
      else {
        val cqField = Text(cqString.substring(0, fieldSepPos))
        ingestRow(BtableFieldT!!, cqField, tconf.cf, tconf.colDegT, if (tconf.degreeUseValue) v else VALONE)
      }
    }
  }

  /** In addiiton to [.ingestRow], handles edge table.  */
  fun ingestRow(rowID: Text, cq: Text, v: Value, edgeID: Text) {
    ingestRow(rowID, cq, v)
    if (tconf.useEdgeTable || tconf.useEdgeTableT) {
      val out = Text(concatBytes(tconf.colDeg.bytes, FIELD_SEPERATOR.toString().toByteArray(StandardCharsets.UTF_8), rowID.bytes))
      val `in` = Text(concatBytes(tconf.colDegT.bytes, FIELD_SEPERATOR.toString().toByteArray(StandardCharsets.UTF_8), cq.bytes))
      if (tconf.useEdgeTable) {
        ingestRow(BtableEdge!!, edgeID, tconf.cf, out, v)
        ingestRow(BtableEdge!!, edgeID, tconf.cf, `in`, v)
      }
      if (tconf.useEdgeTableT) {
        ingestRow(BtableEdgeT!!, out, tconf.cf, edgeID, v)
        ingestRow(BtableEdgeT!!, `in`, tconf.cf, edgeID, v)
      }
    }
    if (tconf.useEdgeTableDegT) {
      ingestRow(BtableEdgeDegT!!, rowID, tconf.cf, tconf.colDeg, if (tconf.degreeUseValue) v else VALONE)
      ingestRow(BtableEdgeDegT!!, cq, tconf.cf, tconf.colDegT, if (tconf.degreeUseValue) v else VALONE)
    }
  }

  companion object : Loggable {
    override val logger: Logger = logger<D4MTableWriter>()

    val DEFAULT_DEGCOL = Text("deg")
    val VALONE = Value("1".toByteArray(StandardCharsets.UTF_8))
    val EMPTYCF = Text("")


    private val ITER_SUMALL_NAME = "sumAll"

    /** Put a SummingIterator on all columns.  */
    fun assignDegreeAccumulator(tableName: String, c: Connector) {
      var cfg: IteratorSetting? = null
      try {
        cfg = c.tableOperations().getIteratorSetting(tableName, ITER_SUMALL_NAME, IteratorUtil.IteratorScope.scan)
      } catch (ignored: AccumuloSecurityException) {

      } catch (ignored: AccumuloException) {
      } catch (e: TableNotFoundException) {
        logger.warn(tableName + " does not exist", e)
      }

      if (cfg != null) {
        logger.info("table " + tableName + ": iterator " + ITER_SUMALL_NAME + " already exists with priority " + cfg.priority + " and options: " + cfg.options)

      } else {
        cfg = IteratorSetting(2, ITER_SUMALL_NAME, SummingCombiner::class.java)
        //Combiner.setColumns(cfg, columnList);
        Combiner.setCombineAllColumns(cfg, true)
        LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING)
        try {
          //c.tableOperations().checkIteratorConflicts(tableName, cfg, EnumSet.allOf(IteratorUtil.IteratorScope.class));
          c.tableOperations().attachIterator(tableName, cfg)
        } catch (e: AccumuloSecurityException) {
          logger.warn("error trying to add $ITER_SUMALL_NAME iterator to $tableName", e)
        } catch (e: AccumuloException) {
          logger.warn("error trying to add $ITER_SUMALL_NAME iterator to $tableName", e)
        } catch (e: TableNotFoundException) {
          logger.warn(tableName + " does not exist", e)
        }

      }
    }

    /** Create a table if not already existing. Return whether table created.  */
    fun createTableSoft(tableName: String, c: Connector, deleteExistingTable: Boolean): Boolean {
      val to = c.tableOperations()
      try {
        if (to.exists(tableName)) {
          if (deleteExistingTable)
            to.delete(tableName)
          else
            return false
        }
        to.create(tableName)
        return true
      } catch (e: AccumuloException) {
        logger.warn("error creating table " + tableName, e)
        return false
      } catch (e: AccumuloSecurityException) {
        logger.warn("error creating table " + tableName, e)
        return false
      } catch (e: TableExistsException) {
        logger.error("impossible! Table checked to be created!", e)
        return false
      } catch (e: TableNotFoundException) {
        logger.error("impossible! Table checked to be created!", e)
        return false
      }

    }

    val FIELD_SEPERATOR = '|'

    internal fun concatBytes(vararg bs: ByteArray): ByteArray {
      var totlen = 0
      for (b in bs)
        totlen += b.size
      val ret = ByteArray(totlen)
      var pos = 0
      for (b in bs) {
        System.arraycopy(b, 0, ret, pos, b.size)
        pos += b.size
      }
      return ret
    }

    fun ingestRow(bw: BatchWriter, rowID: Text, cf: Text, cq: Text, v: Value) {
      val m = Mutation(rowID)
      m.put(cf, cq, v)
      try {
        bw.addMutation(m)
      } catch (e: MutationsRejectedException) {
        logger.warn("mutation rejected: (row,cf,cq,v)=($rowID,$cf,$cq,$v)", e)
      }

    }
  }
}
