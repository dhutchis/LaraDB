package edu.washington.cs.laragraphulo.mr

import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.logger
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.conf.AccumuloConfiguration
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.trace.DistributedTrace
import org.apache.accumulo.core.trace.Trace
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.htrace.HTraceConfiguration
import org.apache.htrace.SpanReceiver
import java.io.IOException
import org.slf4j.Logger


object TracerHolder : Loggable {
  override val logger: Logger = logger<MatMulJob>()

  @JvmStatic
  fun initialize(hostname: String?, service: String, conf: ClientConfiguration) {
    if (receivers.isNotEmpty()) {
      println("already loaded receivers $receivers")
      return
    }
    buildHTraceConf(hostname, service, conf)
  }


  fun buildHTraceConf(hostname: String?, service: String, conf: ClientConfiguration) {
    val spanReceivers = conf.get(ClientConfiguration.ClientProperty.TRACE_SPAN_RECEIVERS)
    val zookeepers = conf.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST)
    val timeout = AccumuloConfiguration.getTimeInMillis(conf.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_TIMEOUT));
    val zkPath = conf.get(ClientConfiguration.ClientProperty.TRACE_ZK_PATH)
    val properties = conf.getAllPropertiesWithPrefix(ClientConfiguration.ClientProperty.TRACE_SPAN_RECEIVER_PREFIX)
    buildHTraceConf_2(hostname, service, spanReceivers, zookeepers, timeout, zkPath, properties)
  }

  private fun buildHTraceConf_2(hostname: String?, service: String?, spanReceivers: String, zookeepers: String, timeout: Long, zkPath: String,
                                properties: Map<String, String>) {
    val conf = Configuration(false)
    conf.set(Property.TRACE_SPAN_RECEIVERS.toString(), spanReceivers)

    // remaining properties will be parsed through an HTraceConfiguration by SpanReceivers
    setProperty(conf, DistributedTrace.TRACER_ZK_HOST, zookeepers)
    setProperty(conf, DistributedTrace.TRACER_ZK_TIMEOUT, timeout.toInt())
    setProperty(conf, DistributedTrace.TRACER_ZK_PATH, zkPath)
    for ((key, value) in properties) {
      setProperty(conf, key.substring(Property.TRACE_SPAN_RECEIVER_PREFIX.key.length), value)
    }
    if (hostname != null) {
      setProperty(conf, DistributedTrace.TRACE_HOST_PROPERTY, hostname)
    }
    if (service != null) {
      setProperty(conf, DistributedTrace.TRACE_SERVICE_PROPERTY, service)
    }
    org.apache.htrace.Trace.setProcessId(service)
    ShutdownHookManager.get().addShutdownHook({
      Trace.off()
      closeReceivers()
    }, 0)
    loadSpanReceivers(conf)
  }

  private fun setProperty(conf: Configuration, key: String, value: String) {
    conf.set(HTRACE_CONF_PREFIX + key, value)
  }

  private fun setProperty(conf: Configuration, key: String, value: Int) {
    conf.setInt(HTRACE_CONF_PREFIX + key, value)
  }

  private fun wrapHadoopConf(conf: Configuration): HTraceConfiguration {
    return object : HTraceConfiguration() {
      override fun get(key: String): String? {
        return conf.get(HTRACE_CONF_PREFIX + key)
      }

      override fun get(key: String, defaultValue: String?): String? {
        return conf.get(HTRACE_CONF_PREFIX + key, defaultValue)
      }
    }
  }

  @Synchronized private fun closeReceivers() {
    for (rcvr in receivers) {
      try {
        rcvr.close()
      } catch (e: IOException) {
        logger.warn("Unable to close SpanReceiver correctly: {}", e.message, e)
      }

    }
    receivers.clear()
  }

  private val receivers = java.util.HashSet<SpanReceiver>()


  @Synchronized private fun loadSpanReceivers(conf: Configuration) {
    println("conf is: ")
    for ((k, v) in conf) {
      println("$k = $v")
    }
    if (!receivers.isEmpty()) {
      logger.info("Already loaded span receivers, enable tracing does not need to be called again")
      return
    }
    val receiverNames = conf.getTrimmedStrings(Property.TRACE_SPAN_RECEIVERS.toString())
    if (receiverNames == null || receiverNames.isEmpty()) {
      return
    }
    for (className in receiverNames) {
      val builder = SpanReceiverBuilder(wrapHadoopConf(conf))
      val rcvr = builder.spanReceiverClass(className.trim { it <= ' ' }).build()
      if (rcvr == null) {
        logger.warn("Failed to load SpanReceiver " + className)
      } else {
        receivers.add(rcvr)
        logger.info("SpanReceiver $className was loaded successfully.")
      }
    }
    for (rcvr in receivers) {
      org.apache.htrace.Trace.addReceiver(rcvr)
    }
  }

  private val HTRACE_CONF_PREFIX = "hadoop."

}