package edu.washington.cs.laragraphulo

import edu.washington.cs.laragraphulo.util.TripleFileWriter
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger

import java.io.File
import java.io.FileNotFoundException
import java.net.URL

/**
 * Insert graph data from a file in test/resources/
 * into Accumulo and test Graphulo functions.
 */
class InsertDataTest : AccumuloTestBase() {

  init {
    logger.debug("Initializing InsertDataTest")
  }

  @Test
  @Throws(FileNotFoundException::class)
  fun putSCALE10() {
    val conn = tester.accumuloConfig.connector
    val baseName = PREFIX_TABLE_SCALE + "10A"

    val tripleFileWriter = TripleFileWriter(conn)
    var url: URL = Thread.currentThread().contextClassLoader.getResource("data/10Ar.txt")
    Assert.assertNotNull(url)
    val rowFile = File(url.path)
    url = Thread.currentThread().contextClassLoader.getResource("data/10Ac.txt")
    Assert.assertNotNull(url)
    val colFile = File(url.path)

    // deleteExistingTables
    val cnt = tripleFileWriter.writeTripleFile_Adjacency(rowFile, colFile, null, ",", baseName, true, false)
    logger.info("Wrote $cnt triples to D4M tables with base name $baseName")
    logger.info { "These tables exist: ${conn.tableOperations().list()}" }
  }

  companion object : Loggable {
    override val logger: Logger = logger<InsertDataTest>()

    val PREFIX_TABLE_SCALE = "test_SCALE_"
  }


}
