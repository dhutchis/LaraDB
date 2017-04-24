package edu.washington.cs.laragraphulo.sensor

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import java.io.File
import java.util.*


class SensorCalcDriver(
    val filesA: Set<File>,
    val filesB: Set<File>,
    val tA: String, // appends "_enc" for encoded version
    val tB: String,
    val minTime: Long,
    val maxTime: Long,
    val cPartitions: Int,
    val conn: Connector,
    val password: PasswordToken
) {
  val tAenc = tA+"_enc"
  val tBenc = tB+"_enc"
  private var ingested = false
  private var ingestedEnc = false

  fun ingest(encode: Boolean) {
    if (!encode && ingested || encode && ingestedEnc) return

    val lockDir = File(if (encode) "lockEnc" else "lockNoEnc")
    listOf(filesA to if (!encode) tA else tAenc, filesB to if (!encode) tB else tBenc).forEach { (files, t) ->
      SensorFileListInsert(files, conn, t, encode, lockDir).run()
    }

    if (!encode) ingested = true
    else ingested = true
  }

  fun doPipeline(opts: Set<SensorCalc.SensorOpt>): SensorCalc.SensorCalcTimes {
    val encode = SensorCalc.SensorOpt.Encode in opts
    val scc = SensorCalc(conn, password,
        if (!encode) tA else tAenc, if (!encode) tB else tBenc,
        opts, cPartitions)
    val times = scc.timeAll(minTime, maxTime)
    println(times)
    scc.deleteAll()
    return times
  }

  fun doMany() {
    ingest(false)
    ingest(true)
//    val noOpts: Set<SensorCalc.SensorOpt> = EnumSet.noneOf(SensorCalc.SensorOpt::class.java)
//    val listStart = listOf(doPipeline(noOpts))

    val optList = listOf(
        "",
        "M",
        "R",
        "P",
        "F",
        "E",
        "D",
        "S",
        "Z",
	"A",
	"ADEFMPRSZ"
    ).map { Thread.sleep(25000); SensorCalc.SensorOpt.parseRep(it) }

    val results = optList.map { 
println(it)
doPipeline(it) }

//    val (_, results) = SensorCalc.SensorOpt.values().fold(noOpts to listStart) { pair, opt ->
//      if (opt == SensorCalc.SensorOpt.MonotoneSortElim)
//        pair    // skip M - seems to be bad?
//      else {
//        val (opts, list) = pair
//        val newOpts = opts + opt
//        val newList = list + doPipeline(newOpts)
//        newOpts to newList
//      }
//    }

    results.forEach(::println)
  }

}

