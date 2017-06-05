package edu.washington.cs.laragraphulo.api

import org.junit.Assert.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.slf4j.Logger
import java.io.File
import java.util.*

class TupleEncodeFieldTest {

  companion object : Loggable {
    override val logger: Logger = logger<TupleEncodeFieldTest>()

    val pschema0 = listOf<PAttribute<*>>()
    val tuples0 = listOf(mapOf<String,Any?>())
    val pschemaS = listOf(PAttribute.PAttributeImpl("id",PType.INT))
    val tuplesS = listOf(mapOf("id" to 0))
    val pschema1 = listOf(PAttribute.PAttributeImpl("id",PType.INT),PAttribute.PAttributeImpl("t",PType.LONG))
    val tuples1 = listOf(mapOf("id" to 0, "t" to 1L),mapOf("id" to 1, "t" to Long.MAX_VALUE),mapOf("id" to -1, "t" to 0L))
    val pschema2 = listOf(PAttribute.PAttributeImpl("t",PType.LONG),PAttribute.PAttributeImpl("c",PType.STRING))
    val tuples2 = listOf(mapOf("t" to 1L, "c" to ""),mapOf("t" to Long.MAX_VALUE, "c" to "high"),mapOf("t" to 0L, "c" to "ZERO"))
    val pschema2r = pschema2.reversed()
    val pschema3 = listOf(PAttribute.PAttributeImpl("c",PType.STRING),PAttribute.PAttributeImpl("t",PType.LONG_VARIABLE))
    val tuples3 = listOf(mapOf("t" to 1L, "c" to ""),mapOf("t" to Long.MAX_VALUE, "c" to "high"),mapOf("t" to 0L, "c" to "ZERO"))
    val pschema4 = listOf(PAttribute.PAttributeImpl("t",PType.LONG),PAttribute.PAttributeImpl("c",PType.STRING), PAttribute.PAttributeImpl("v", PType.DOUBLE))
    val tuples4 = listOf(mapOf("t" to 1L, "c" to "", "v" to 0.0),mapOf("t" to Long.MAX_VALUE, "c" to "high", "v" to 1.0),mapOf("t" to 0L, "c" to "ZERO", "v" to -3.5),mapOf("t" to -1L, "c" to "", "v" to -3.5))
    @JvmStatic @Suppress("UNUSED")
    fun testCases() = listOf(pschema0 to tuples0, pschemaS to tuplesS, pschema1 to tuples1, pschema2 to tuples2, pschema2r to tuples2, pschema3 to tuples3, pschema4 to tuples4)
    @JvmStatic @Suppress("UNUSED")
    fun extraDataCases() = listOf(pschema2 to tuples4, pschema2r to tuples4, pschema3 to tuples4)

//    val specialTuples = listOf<Map<String,*>>()
    fun getSpecialTuples(): List<Map<String,*>> {
      val url = Thread.currentThread().contextClassLoader.getResource("data/sensor/testTCCP.tsv")
//      println(url.path)
      val f = File(url.path)
      require(f.exists())
      return f.readLines().let { lines ->
        lines.map { line ->
          val split = line.split('\t')
          require(split.size == 3) { "bad line: $line" }
          val tp = split[0].toLong()
          val c = split[1]
          val cp = split[2]
          mapOf("t'" to tp, "c" to c, "c'" to cp)
        }
      }
    }

  }

  @ParameterizedTest
  @MethodSource(names = arrayOf("testCases"))
  fun testEncodeRow(pair: Pair<List<PAttribute<*>>, List<Tuple>>) {
    val (attrs,tuples) = pair
    for (tuple in tuples) {
      logger.debug{"before : $tuple"}
      val encoded: ByteArray = TupleByKeyValue.encodeJoin(attrs, tuple)
      logger.debug{"encoded: ${Arrays.toString(encoded)}"}
      val decoded: List<Lazy<Any?>> = TupleByKeyValue.decodeSplit(attrs, ABS(encoded))
      val decodedPaired = attrs.zip(decoded).map { (attr,f) -> attr.name to f.value }.toMap()
      logger.debug{"decoded: $decodedPaired"}
      assertEquals(tuple, decodedPaired)
    }
  }

  @ParameterizedTest
  @MethodSource(names = arrayOf("testCases"))
  fun testEncodeRowNullable(pair: Pair<List<PAttribute<*>>, List<Tuple>>) {
    val (attrs0,tuples0) = pair
    val attrs = attrs0.map { PAttribute(it.name, it.type.nullable) }
    val tuples: List<Tuple> = tuples0 + mapOf<Name,Any?>()
    for (tuple in tuples) {
      logger.debug{"before : $tuple"}
      val encoded: ByteArray = TupleByKeyValue.encodeJoin(attrs, tuple)
      logger.debug{"encoded: ${Arrays.toString(encoded)}"}
      val decoded: List<Lazy<Any?>> = TupleByKeyValue.decodeSplit(attrs, ABS(encoded))
      val decodedPaired = attrs.zip(decoded).map { (attr,f) -> attr.name to f.value }.toMap().filterValues { it != null }
      logger.debug{"decoded: $decodedPaired"}
      assertEquals(tuple, decodedPaired)
    }
  }

  @Test
  fun testNoAttrs() {
    val tuple = tuples1[0]
    val attrs = pschema0
    logger.debug{"before : $tuple"}
    val encoded: ByteArray = TupleByKeyValue.encodeJoin(attrs, tuple)
    logger.debug{"encoded: ${Arrays.toString(encoded)}"}
    assertTrue(encoded.isEmpty())
    val decoded: List<Lazy<Any?>> = TupleByKeyValue.decodeSplit(attrs, ABS(encoded))
    assertTrue(decoded.isEmpty())
    val decodedPaired = attrs.zip(decoded).map { (attr,f) -> attr.name to f.value }.toMap()
    logger.debug{"decoded: $decodedPaired"}
    assertTrue(decodedPaired.isEmpty())
  }

  @ParameterizedTest
  @MethodSource(names = arrayOf("extraDataCases"))
  fun testExtraData(pair: Pair<List<PAttribute<*>>, List<Tuple>>) {
    val (attrs,tuples) = pair
    for (tuple in tuples) {
      logger.debug{"before : $tuple"}
      val encoded: ByteArray = TupleByKeyValue.encodeJoin(attrs, tuple)
      logger.debug{"encoded: ${Arrays.toString(encoded)}"}
      val decoded: List<Lazy<Any?>> = TupleByKeyValue.decodeSplit(attrs, ABS(encoded))
      val decodedPaired = attrs.zip(decoded).map { (attr,f) -> attr.name to f.value }.toMap()
      logger.debug{"decoded: $decodedPaired"}
      assertEquals(tuple.filterKeys { it in listOf("t","c") }, decodedPaired)
    }
  }

  @Test
  fun testSpecial() {
    val attrs = listOf(PAttribute("c",PType.STRING), PAttribute("c'",PType.STRING), PAttribute("t'",PType.LONG))
    val enc = byteArrayOf(65, 80, 68, 83, 45, 57, 48, 48, 54, 45, 48, 50, 48, 59, 105, 110, 116, 101, 110, 115, 105, 116, 121, 0, 65, 80, 68, 83, 45, 57, 48, 48, 54, 45, 48, 50, 48, 59, 105, 110, 116, 101, 110, 115, 105, 116, 121, 0, 1, 0, 1, 0, 1, 1, 90, 19, 59, 7, 64)
    val decList = TupleByKeyValue.decodeSplit(attrs, enc)
    decList.map { it.value }.forEach { println(it) }
  }

  @Test
  fun testSpecial2() {
    val attrs = listOf(PAttribute("c",PType.STRING), PAttribute("c'",PType.STRING), PAttribute("t'",PType.LONG))
    for (tuple in getSpecialTuples()) {
//      logger.debug{"before : $tuple"}
      val encoded: ByteArray = TupleByKeyValue.encodeJoin(attrs, tuple)
//      logger.debug{"encoded: ${Arrays.toString(encoded)}"}
      val decoded: List<Lazy<Any?>> = TupleByKeyValue.decodeSplit(attrs, ABS(encoded))
      val decodedPaired = attrs.zip(decoded).map { (attr,f) -> attr.name to f.value }.toMap()
//      logger.debug{"decoded: $decodedPaired"}
      assertEquals(tuple, decodedPaired)
    }
  }

}