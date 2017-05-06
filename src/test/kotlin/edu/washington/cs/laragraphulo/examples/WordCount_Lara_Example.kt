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
 * Example demonstrating a simple Lara Ext and Union (Agg) function call using the Lara API.
 *
 * Run this example via `mvn test -Dtest=WordCount_Lara_Example`.
 * The test does not use Accumulo.
 */
class WordCount_Lara_Example {

  /** The main entry point of this example. START READING HERE. */
  @Test
  fun wordCountExample() {
    // ============= INPUT DATA
    val exampleData = listOf(
        /* Row to Value */
        "doc1" to "she sells sea shells by the sea shore",
        "doc2" to "the shells she sells are surely seashells",
        "doc3" to "so if she sells shells on the seashore",
        "doc4" to "i’m sure she sells seashore shells"
    ).map { (docid,text) -> mapOf("docid" to docid, "text" to text) }

    // ============= ATTRIBUTES
    val attrD = Attribute("docid", STRING)
    val attrT = ValAttribute("text", STRING, "")
    val attrW = Attribute("word", STRING)
    val attrCount = ValAttribute("count", UINT, 0)

    // ============= SCHEMA
    val initialSchema = NameSchema(
        keys = listOf(attrD),
        vals = listOf(attrT)
    )

    // ============= UDF
    val extFun = ExtFun(NameSchema(listOf(attrW), listOf(attrCount))) { tuple ->
      val wordMap: Map<String, Int> = (tuple["text"] as String).split(" ").groupBy { it }.mapValues { (_,list) -> list.count() }
      wordMap.map { (word,count) -> mapOf("word" to word, "count" to count) }
    }
    val plus = PlusFun(0, Int::plus)

    // ============= QUERY
    val query = ScanFromData(initialSchema, exampleData)
        .ext(extFun)
        .sort(listOf("word", "docid"))
        .agg(listOf("word"), mapOf("count" to plus))

    // ============= RUN! (and print the result)
    val scannedData = query.run().toList()
    scannedData.forEach { logger.debug {it} }

//    // ============= CHECK CORRECTNESS
//    val expectedResult = exampleData.map {map -> map + ("v" to map["v"]!!.replace(RAINY, SUNNY)) }
//    Assert.assertEquals(scannedData, expectedResult)
  }
  

  
  
  /** Static variables and methods */
  companion object : Loggable {
    /** This is used for logging messages */
    override val logger: Logger = logger<WordCount_Lara_Example>()

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