package edu.washington.cs.laragraphulo.examples

import edu.washington.cs.laragraphulo.Loggable
import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.api.NameTupleOp.*
import edu.washington.cs.laragraphulo.api.LType.*
import edu.washington.cs.laragraphulo.debug
import edu.washington.cs.laragraphulo.logger
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger
import java.util.*


/**
 * Example demonstrating a simple Lara Ext function call using the Lara API.
 *
 * Run this example via `mvn test -Dtest=HelloWorld_Lara_Example`.
 * The test does not use Accumulo.
 */
class HelloWorld_Lara_Example {

  /** The main entry point of this example. START READING HERE. */
  @Test
  fun helloWorldExample() {
    // ============= INPUT DATA
    val exampleData = listOf(
        /* Row to Value */
        "msg1" to "Hello World!",
        "msg2" to "Hello Glorious World!",
        "msg3" to "Hello Rainy World!"
    ).map { (msgnum,msg) -> mapOf("k" to msgnum, "v" to msg) }

    // ============= ATTRIBUTES
    val attrK = Attribute("k", STRING)
    val attrV = ValAttribute("v", STRING, "")

    // ============= SCHEMA
    val initialSchema = NameSchema(
        keys = listOf(attrK),
        vals = listOf(attrV)
    )

    // ============= UDF
    val mapFun = MapFun(listOf(attrV)) { tuple ->
      mapOf("v" to (tuple["v"] as String).replace(RAINY, SUNNY))
    }

    // ============= QUERY
    val query = ScanFromData(initialSchema, exampleData)
        .ext(mapFun)

    // ============= RUN! (and print the result)
    val scannedData = query.run().toList()
    scannedData.forEach { logger.debug {it} }

    // ============= CHECK CORRECTNESS
    val expectedResult = exampleData.map {map -> map + ("v" to map["v"]!!.replace(RAINY, SUNNY)) }
    Assert.assertEquals(scannedData, expectedResult)
  }
  

  
  
  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<HelloWorld_Lara_Example>()

    const val RAINY = "Rainy"
    const val SUNNY = "Sunny"


    fun <T> Iterator<T>.toList(): List<T> {
      val l = LinkedList<T>()
      while (this.hasNext())
        l += this.next()
      return l
    }
  }
}