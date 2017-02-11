package edu.washington.cs.laragraphulo.sensor

import org.apache.accumulo.core.client.Connector

/**
 * Setup [SensorCalc] with appropriate options.
 */
class SensorCalcConfig(
    val conn: Connector,
    val tA: String,
    val tB: String,
    val optSet: Set<SensorOpt>
) {

  enum class SensorOpt {

  }

}