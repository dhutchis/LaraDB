package edu.washington.cs.laragraphulo

import edu.washington.cs.laragraphulo.opt.AccumuloConfig
import edu.washington.cs.laragraphulo.opt.AccumuloConfigImpl
import org.apache.accumulo.core.client.*
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.junit.rules.ExternalResource

class RealAccumuloTester(instanceName: String, zookeeperHost: String,
                         username: String, password: PasswordToken) : ExternalResource(), AccumuloTester {

  private val cc: ClientConfiguration

  init {
    cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost) // .withZkTimeout(timeout)
    logger.debug { "Using parameters from $cc" }
  }

  override val accumuloConfig: AccumuloConfig = AccumuloConfigImpl(
      cc.get(ClientConfiguration.ClientProperty.INSTANCE_NAME),
      cc.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST),
      username, password)

  override fun toString(): String = "RealAccumuloTester(${cc.get(ClientConfiguration.ClientProperty.INSTANCE_NAME)})"

  companion object : Loggable {
    override val logger: org.slf4j.Logger = logger<RealAccumuloTester>()
  }

}
