package edu.washington.cs.laragraphulo

import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import org.apache.accumulo.core.client.*
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.accumulo.minicluster.MiniAccumuloConfig
import org.junit.rules.ExternalResource
import org.slf4j.Logger

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.attribute.FileAttribute

/**
 * Provides a MiniAccumuloCluster for use in testing.
 */
class MiniAccumuloTester(
    private val numTservers: Int,
    private val doDebug: Boolean,
    private val reuse: Boolean
) : ExternalResource(), AccumuloTester {
  /* Fixture State */
  private var tempDir: File? = null
  private var miniaccumulo: MiniAccumuloCluster? = null
  private var _ac: AccumuloConfig? = null


  override val accumuloConfig: AccumuloConfig
    get() = _ac!!

  @Throws(Throwable::class)
  override fun before() {
    if (_ac == null) {
//      val sw = StopWatch()
//      sw.start()
      tempDir = Files.createTempDirectory("tempMini", *arrayOf<FileAttribute<*>>()).toFile()
      logger.info("Temp directory: " + tempDir!!.path)

      val mac = MiniAccumuloConfig(tempDir, PASSWORD).setNumTservers(numTservers)
      mac.isJDWPEnabled = doDebug
      val ma = MiniAccumuloCluster(mac)
      miniaccumulo = ma
      ma.start()

      /*******************************************************************
       * MiniAccumulo DEBUG Section. Instructions:
       * Watch the test output with `tail -f `
       * When you see the debug port appear on screen for TABLET_SERVER,
       * connect to that port with your IDE.
       * You have 10 seconds before the test continues.
       */
      if (doDebug) {
        println("DEBUG PORTS: " + miniaccumulo!!.debugPorts)
        Thread.sleep(10000)
      }

      _ac = AccumuloConfig(ma.instanceName, ma.zooKeepers, USER, PasswordToken(PASSWORD))
//      sw.stop()
      logger.debug("MiniAccumulo created instance: ${ma.instanceName} ")//- creation time: ${sw.getTime() / 1000.0}s")
    } else
      logger.debug("Reusing MiniAccumulo instance " + _ac!!.instanceName)
  }

  override fun after() {
    if (!reuse && miniaccumulo != null) {
      _ac = null
      try {
        miniaccumulo!!.stop()
      } catch (e: IOException) {
        System.err.print("Error stopping MiniAccumuloCluster: ")
        e.printStackTrace()
      } catch (e: InterruptedException) {
        System.err.print("Error stopping MiniAccumuloCluster: ")
        e.printStackTrace()
      }

      val b = tempDir!!.delete()
      miniaccumulo = null
      logger.debug("tearDown ok - instance destroyed; tempDir deleted=" + b)
    }
  }

  @Suppress("ProtectedInFinal", "unused")
  @Throws(Throwable::class)
  protected fun finalize() {
    if (miniaccumulo != null) {
      try {
        miniaccumulo!!.stop()
      } catch (e: IOException) {
        System.err.print("Error stopping MiniAccumuloCluster in finalize: ")
        e.printStackTrace()
      } catch (e: InterruptedException) {
        System.err.print("Error stopping MiniAccumuloCluster in finalize: ")
        e.printStackTrace()
      }

      val b = tempDir!!.delete()
      miniaccumulo = null
      logger.debug("FINALIZE tearDown ok - instance destroyed; tempDir deleted=" + b)
    }
  }

  override fun toString(): String = "MiniAccumuloTester(numTservers=$numTservers, reuse=$reuse)"


  companion object : Loggable {
    override val logger: Logger = logger<MiniAccumuloTester>()
    private val USER = "root"
    private val PASSWORD = "password"
  }
}
