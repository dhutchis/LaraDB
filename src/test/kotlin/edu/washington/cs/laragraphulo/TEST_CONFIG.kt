package edu.washington.cs.laragraphulo

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.*


/**
 * Holds test configurations; default test suite for JUnit testing.
 */
object TEST_CONFIG {
  val logger: Logger = LoggerFactory.getLogger(this.javaClass)

  @JvmStatic
  val accumuloTester: AccumuloTester
    get() = _accumuloTester!!

  /**
   * Set the Accumulo config to use for all test classes here.
   */
  var _accumuloTester: AccumuloTester? = null
    private set

  val DEFAULT_TEST_CONFIG_FILE = "GraphuloTest.conf"

  init {
    val s = System.getProperty("TEST_CONFIG") // environment variable
    if (s == null && File(DEFAULT_TEST_CONFIG_FILE).exists()) {
      loadConfigFromFileOrSystem(DEFAULT_TEST_CONFIG_FILE)
      if (_accumuloTester == null)
        _accumuloTester = MiniAccumuloTester(1, false, false)
    } else if (s == null) {
      _accumuloTester = MiniAccumuloTester(1, false, false)
    } else if (s.startsWith("txe1-")) {
      val instance = s.substring(5)
      val file = File("/home/gridsan/groups/databases/$instance/accumulo_user_password.txt")
      var token: PasswordToken? = null
      try {
        BufferedReader(FileReader(file)).use { `is` -> token = PasswordToken(`is`.readLine()) }
      } catch (e: FileNotFoundException) {
        logger.error("Cannot find accumulo_user_password.txt for instance " + instance, e)
        throw RuntimeException(e)
      } catch (e: IOException) {
        logger.error("Problem reading accumulo_user_password.txt for instance " + instance, e)
        throw RuntimeException(e)
      }

      _accumuloTester = RealAccumuloTester(instance, instance + ".cloud.llgrid.txe1.mit.edu:2181", "AccumuloUser", token!!)

    } else {
      when (s) {
        "local" ->
          // G = edu.mit.ll.graphulo.MatlabGraphulo('instance','localhost:2181','root','secret')
          // DB = DBserver('localhost:2181','Accumulo','instance','root','secret')
          _accumuloTester = RealAccumuloTester("instance", "localhost:2181", "root", PasswordToken("secret"))
        "local-1.7" ->
          // G = edu.mit.ll.graphulo.MatlabGraphulo('instance-1.7.0','localhost:2181','root','secret')
          // DB = DBserver('localhost:2181','Accumulo','instance-1.7.0','root','secret')
          _accumuloTester = RealAccumuloTester("instance-1.7.0", "localhost:2181", "root", PasswordToken("secret"))
        "mini" -> _accumuloTester = MiniAccumuloTester(1, false, false)
        "miniDebug"   // Enables debugging on started MiniAccumulo process.
        -> _accumuloTester = MiniAccumuloTester(1, true, false)
        "mini2" // 2 tablet server MiniAccumuloCluster
        -> _accumuloTester = MiniAccumuloTester(2, false, false)
        "miniReuse" -> _accumuloTester = MiniAccumuloTester(2, false, true)
        else -> {
          // interpret value as a file path
          loadConfigFromFileOrSystem(s)
          if (_accumuloTester == null)
            _accumuloTester = MiniAccumuloTester(1, false, false)
        }
      }
    }
    logger.info("Using $accumuloTester")
  }

  val KEY_INSTANCE_NAME = "accumulo.it.cluster.standalone.instance.name"
  val KEY_ZOOKEEPERS = "accumulo.it.cluster.standalone.zookeepers"
  val KEY_USER = "accumulo.it.cluster.standalone.admin.principal"
  val KEY_PASSWORD = "accumulo.it.cluster.standalone.admin.password"

  private fun loadConfigFromFileOrSystem(filename: String?) {
    var instancename: String? = null
    var zookeepers: String? = null
    var user: String? = null
    var pass: String? = null
    if (filename != null && !filename.isEmpty())
      try {
        val properties = PropertiesConfiguration(filename)
        instancename = properties.getString(KEY_INSTANCE_NAME)
        zookeepers = properties.getString(KEY_ZOOKEEPERS)
        user = properties.getString(KEY_USER)
        pass = properties.getString(KEY_PASSWORD)
      } catch (e: ConfigurationException) {
        logger.warn("Couldn't find a valid properties file named " + filename)
      }

    instancename = System.getProperty(KEY_INSTANCE_NAME, instancename)
    zookeepers = System.getProperty(KEY_ZOOKEEPERS, zookeepers)
    user = System.getProperty(KEY_USER, user)
    pass = System.getProperty(KEY_PASSWORD, pass)
    if (instancename == null || zookeepers == null || user == null || pass == null)
      return
    _accumuloTester = RealAccumuloTester(instancename, zookeepers, user, PasswordToken(pass))
  }

  // Alternatives:
  //    public static final IAccumuloTester AccumuloTester =
  //            new RealAccumuloTester("instance","localhost:2181",5000,"root",new PasswordToken("secret"));

  //"classdb51.cloud.llgrid.txe1.mit.edu:2181"

}
