package edu.washington.cs.laragraphulo.util

import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.client.*
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.Text
import org.slf4j.Logger
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.util.Scanner
import java.util.zip.GZIPInputStream

/**
 * Write row, column and (optionally) value files to a table.
 */
class TripleFileWriter(private val connector: Connector) {

  /**
   * Writes triples from component files to a main table, transpose table and degree table.

   * @param valFile Optional value file. Uses "1" if not given.
   * @param delimiter Delimiter that separates items.
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * @param deleteExistingTables Delete tables if present.
   * @param trackTime Log the rate of ingest or not.
   * @return Number of triples written.
   */
  fun writeTripleFile_Adjacency(rowFile: File, colFile: File, valFile: File?,
                                delimiter: String, baseName: String,
                                deleteExistingTables: Boolean, trackTime: Boolean): Long {
    var count: Long = 0
    var startTime: Long
    var origStartTime: Long
    val row = Text()
    val col = Text()
    var valText: Text? = null
    val `val` = D4MTableWriter.VALONE

    var valScanner: Scanner? = null
    try {
      Scanner(if (rowFile.name.endsWith(".gz")) GZIPInputStream(FileInputStream(rowFile)) else FileInputStream(rowFile)).use { rowScanner ->
        Scanner(if (colFile.name.endsWith(".gz")) GZIPInputStream(FileInputStream(colFile)) else FileInputStream(colFile)).use { colScanner ->
          rowScanner.useDelimiter(delimiter)
          colScanner.useDelimiter(delimiter)
          if (valFile != null) {
            valScanner = Scanner(valFile)
            valScanner!!.useDelimiter(delimiter)
            valText = Text()
          }

          val config = D4MTableWriter.D4MTableConfig(baseName, connector)
          config.useTable = true
          config.useTableT = true
          config.sumTable = true
          config.sumTableT = true
          config.useTableDeg = true
          config.useTableDegT = true
          config.useTableField = false
          config.useTableFieldT = false
          config.useSameDegTable = true
          config.colDeg = Text("out")
          config.colDegT = Text("in")
          config.deleteExistingTables = deleteExistingTables

          startTime = System.currentTimeMillis()
          origStartTime = startTime
          D4MTableWriter(config).use({ tw ->
            while (rowScanner.hasNext()) {
              if (!colScanner.hasNext() || valScanner != null && !valScanner!!.hasNext()) {
                throw IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                    " rowScanner.hasNext()=" + rowScanner.hasNext() +
                    " colScanner.hasNext()=" + colScanner.hasNext() +
                    if (valScanner == null) "" else " valScanner.hasNext()=" + valScanner!!.hasNext())
              }
              row.set(rowScanner.next())
              col.set(colScanner.next())
              if (valFile != null) {
                valText!!.set(valScanner!!.next())
                `val`.set(valText!!.bytes)
              }
              tw.ingestRow(row, col, `val`)
              count++

              if (trackTime && count % 100000L == 0L) {
                val stopTime = System.currentTimeMillis()
                if (startTime - stopTime > 1000 * 60) {
                  System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime) / 1000,
                      Math.round(count / ((stopTime - origStartTime) / 1000.0)))
                  startTime = stopTime
                }
              }
            }
          })
        }
      }
    } catch (e: IOException) {
      logger.warn("", e)
      throw RuntimeException(e)
    } finally {
      if (valScanner != null)
        valScanner!!.close()
    }
    return count
  }

  /**
   * Writes triples from component files to a main table, transpose table and transpose degree table.

   * @param valFile Optional value file. Uses "1" if not given.
   * *
   * @param delimiter Delimiter that separates items.
   * *
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * *
   * @param deleteExistingTables Delete tables if present.
   * *
   * @param trackTime Log the rate of ingest or not.
   * *
   * @return Number of triples written.
   */
  fun writeTripleFile_Incidence(rowFile: File, colFile: File, valFile: File?,
                                delimiter: String, baseName: String,
                                deleteExistingTables: Boolean, trackTime: Boolean, estimateNumEntries: Long): Long {
    val numBits = (Math.log10(estimateNumEntries.toDouble()) + 1).toInt()

    var count: Long = 0
    var startTime: Long
    var origStartTime: Long
    val row = Text()
    val col = Text()
    var valText: Text? = null
    val `val` = D4MTableWriter.VALONE

    var valScanner: Scanner? = null
    try {
      Scanner(if (rowFile.name.endsWith(".gz")) GZIPInputStream(FileInputStream(rowFile)) else FileInputStream(rowFile)).use { rowScanner ->
        Scanner(if (colFile.name.endsWith(".gz")) GZIPInputStream(FileInputStream(colFile)) else FileInputStream(colFile)).use { colScanner ->
          rowScanner.useDelimiter(delimiter)
          colScanner.useDelimiter(delimiter)
          if (valFile != null) {
            valScanner = Scanner(valFile)
            valScanner!!.useDelimiter(delimiter)
            valText = Text()
          }

          val config = D4MTableWriter.D4MTableConfig(baseName, connector)
          config.useTable = false
          config.useTableT = false
          config.sumTable = true
          config.sumTableT = true
          config.useTableDeg = false
          config.useTableDegT = false
          config.useTableField = false
          config.useTableFieldT = false
          config.useEdgeTable = true
          config.useEdgeTableT = true
          config.useEdgeTableDegT = true
          //      config.useSameDegTable = true;
          config.colDeg = Text("out")
          config.colDegT = Text("in")
          config.deleteExistingTables = deleteExistingTables

          origStartTime = System.currentTimeMillis()
          startTime = origStartTime
          D4MTableWriter(config).use({ tw ->
            while (rowScanner.hasNext()) {
              if (!colScanner.hasNext() || valScanner != null && !valScanner!!.hasNext()) {
                throw IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                    " rowScanner.hasNext()=" + rowScanner.hasNext() +
                    " colScanner.hasNext()=" + colScanner.hasNext() +
                    if (valScanner == null) "" else " valScanner.hasNext()=" + valScanner!!.hasNext())
              }
              row.set(rowScanner.next())
              col.set(colScanner.next())
              if (valFile != null) {
                valText!!.set(valScanner!!.next())
                `val`.set(valText!!.bytes)
              }

              count++
              val edgeID = Text(StringUtils.rightPad(StringUtils.reverse(java.lang.Long.toString(count)), numBits, '0'))

              tw.ingestRow(row, col, `val`, edgeID)


              if (trackTime && count % 100000L == 0L) {
                val stopTime = System.currentTimeMillis()
                if (startTime - stopTime > 1000 * 60) {
                  System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime) / 1000,
                      Math.round(count / ((stopTime - origStartTime) / 1000.0)))
                  startTime = stopTime
                }
              }
            }
          })
        }
      }
    } catch (e: IOException) {
      logger.warn("", e)
      throw RuntimeException(e)
    } finally {
      if (valScanner != null)
        valScanner!!.close()
    }
    return count
  }

  fun writeFromAdjacency_Incidence(baseName: String,
                                   deleteExistingTables: Boolean, trackTime: Boolean, estimateNumEntries: Long): Long {
    val numBits = (Math.log10(estimateNumEntries.toDouble()) + 1).toInt()

    var count: Long = 0
    var startTime: Long
    val origStartTime: Long


    val bs: BatchScanner
    try {
      bs = connector.createBatchScanner(baseName, Authorizations.EMPTY, 2)
    } catch (e: TableNotFoundException) {
      throw RuntimeException("Table $baseName does not exist", e)
    }

    bs.setRanges(setOf(Range()))

    val config = D4MTableWriter.D4MTableConfig(baseName, connector)
    config.useTable = false
    config.useTableT = false
    config.sumTable = true
    config.sumTableT = true
    config.useTableDeg = false
    config.useTableDegT = false
    config.useTableField = false
    config.useTableFieldT = false
    config.useEdgeTable = true
    config.useEdgeTableT = true
    config.useEdgeTableDegT = true
    //      config.useSameDegTable = true;
    config.colDeg = Text("out")
    config.colDegT = Text("in")
    config.deleteExistingTables = deleteExistingTables
    config.degreeUseValue = true

    origStartTime = System.currentTimeMillis()
    startTime = origStartTime
    try {
      D4MTableWriter(config).use({ tw ->

        var outNode = Text()
        var inNode = Text()
        for ((k, value) in bs) {

          outNode = k.getRow(outNode)
          inNode = k.getColumnQualifier(inNode)

          count++
          val edgeID = Text(StringUtils.rightPad(StringUtils.reverse(java.lang.Long.toString(count)), numBits, '0'))

          tw.ingestRow(outNode, inNode, value, edgeID)


          if (trackTime && count % 100000L == 0L) {
            val stopTime = System.currentTimeMillis()
            if (startTime - stopTime > 1000 * 60) {
              System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime) / 1000,
                  Math.round(count / ((stopTime - origStartTime) / 1000.0)))
              startTime = stopTime
            }
          }
        }
      })
    } finally {
      bs.close()
    }
    return count
  }

  /**
   * Writes triples from component files to a main table, transpose table and degree table.

   * @param valFile Optional value file. Uses "1" if not given.
   * *
   * @param delimiter Delimiter that separates items.
   * *
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * *
   * @param deleteExistingTables Delete tables if present.
   * *
   * @param trackTime Log the rate of ingest or not.
   * *
   * @return Number of triples written.
   */
  fun writeTripleFile_Single(rowFile: File, colFile: File, valFile: File?,
                             delimiter: String, baseName: String,
                             deleteExistingTables: Boolean, trackTime: Boolean): Long {
    var count: Long = 0
    val text = Text()
    var valText: Text? = null
    val `val` = D4MTableWriter.VALONE

    var valScanner: Scanner? = null
    try {
      Scanner(if (rowFile.name.endsWith(".gz")) GZIPInputStream(FileInputStream(rowFile)) else FileInputStream(rowFile)).use { rowScanner ->
        Scanner(if (colFile.name.endsWith(".gz")) GZIPInputStream(FileInputStream(colFile)) else FileInputStream(colFile)).use { colScanner ->
          rowScanner.useDelimiter(delimiter)
          colScanner.useDelimiter(delimiter)
          if (valFile != null) {
            valScanner = Scanner(valFile)
            valScanner!!.useDelimiter(delimiter)
            valText = Text()
          }

          val outCol = Text("deg")
          // changed to undirected version
          val inCol = Text("deg")
          val edgeCol = Text("edge")
          val Stable = baseName + "Single"
          val edgeSep = '|'

          if (connector.tableOperations().exists(Stable))
            if (deleteExistingTables)
              connector.tableOperations().delete(Stable)
            else
              return -1 //throw new TableExistsException(Stable+" already exists");
          connector.tableOperations().create(Stable)
          D4MTableWriter.assignDegreeAccumulator(Stable, connector)

          val bwc = BatchWriterConfig()
          val bw = connector.createBatchWriter(Stable, bwc)

          val origStartTime = System.currentTimeMillis()
          var startTime = origStartTime
          try {
            while (rowScanner.hasNext()) {
              if (!colScanner.hasNext() || valScanner != null && !valScanner!!.hasNext()) {
                throw IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                    " rowScanner.hasNext()=" + rowScanner.hasNext() +
                    " colScanner.hasNext()=" + colScanner.hasNext() +
                    if (valScanner == null) "" else " valScanner.hasNext()=" + valScanner!!.hasNext())
              }
              val rowStr = rowScanner.next()
              val colStr = colScanner.next()
              if (valFile != null) {
                valText!!.set(valScanner!!.next())
                `val`.set(valText!!.copyBytes())
              }

              text.set(rowStr)
              var mut = Mutation(text)
              mut.put(D4MTableWriter.EMPTYCF, outCol, `val`)
              bw.addMutation(mut)

              text.set(colStr)
              mut = Mutation(text)
              mut.put(D4MTableWriter.EMPTYCF, inCol, `val`)
              bw.addMutation(mut)

              text.set(rowStr + edgeSep + colStr)
              mut = Mutation(text)
              mut.put(D4MTableWriter.EMPTYCF, edgeCol, `val`)
              bw.addMutation(mut)

              // Force undirected
              text.set(colStr + edgeSep + rowStr)
              mut = Mutation(text)
              mut.put(D4MTableWriter.EMPTYCF, edgeCol, `val`)
              bw.addMutation(mut)

              count++

              if (trackTime && count % 100000L == 0L) {
                val stopTime = System.currentTimeMillis()
                if (startTime - stopTime > 1000 * 60) {
                  System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec%n", count, (stopTime - origStartTime) / 1000,
                      Math.round(count / ((stopTime - origStartTime) / 1000.0)))
                  startTime = stopTime
                }
              }
            }
          } finally {
            bw.close()
          }
        }
      }
    } catch (e: IOException) {
      logger.warn("", e)
      throw RuntimeException(e)
    } catch (e: TableExistsException) {
      logger.error("crazy", e)
      throw RuntimeException(e)
    } catch (e: TableNotFoundException) {
      logger.error("crazy", e)
      throw RuntimeException(e)
    } catch (e: AccumuloSecurityException) {
      logger.warn(" ", e)
      throw RuntimeException(e)
    } catch (e: AccumuloException) {
      logger.warn(" ", e)
      throw RuntimeException(e)
    } finally {
      if (valScanner != null)
        valScanner!!.close()
    }
    return count
  }

  companion object : Loggable {
    override val logger: Logger = logger<TripleFileWriter>()
  }


}
