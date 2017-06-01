package edu.washington.cs.laragraphulo.examples.wordcount

import edu.washington.cs.laragraphulo.api.*
import edu.washington.cs.laragraphulo.api.TupleOp.*
import edu.washington.cs.laragraphulo.api.LType.*


object WordCountQuery {
  const val table1 = "testWordCount1"

  // ============= INPUT DATA
  val exampleData = listOf(
      /* Row to Value */
      "doc1" to "she sells sea shells by the sea shore",
      "doc2" to "the shells she sells are surely seashells",
      "doc3" to "so if she sells shells on the seashore",
      "doc4" to "i'm sure she sells seashore shells"
  ).map { (docid,text) -> mapOf("docid" to docid, "text" to text) }

  // ============= ATTRIBUTES
  val attrD = Attribute("docid", STRING)
  val attrT = ValAttribute("text", STRING, "")
  val attrW = Attribute("word", STRING)
  val attrCount = ValAttribute("count", UINT, 0)

  // ============= SCHEMA
  val initialSchema = Schema(
      keys = listOf(attrD),
      vals = listOf(attrT)
  )

  // ============= UDF
  val extFun = ExtFun("Tokenize", Schema(listOf(attrW), listOf(attrCount))) { tuple ->
    val wordMap: Map<String, Int> = (tuple["text"] as String).split(" ")
        .groupBy { it }.mapValues { (_,list) -> list.count() }
    wordMap.map { (word,count) -> mapOf("word" to word, "count" to count) }
  }
  val plus = PlusFun("Plus", 0, Int::plus)

  // ============= QUERY
  val query = Load(table1, initialSchema)
      .ext(extFun)
      .sort(listOf("word", "docid"))
      .agg(listOf("word"), mapOf("count" to plus))


  // ============= EXPECTED RESULT
  val expectedData = listOf(
      mapOf("count" to 1, "word" to "are"),
      mapOf("count" to 1, "word" to "by"),
      mapOf("count" to 1, "word" to "i'm"),
      mapOf("count" to 1, "word" to "if"),
      mapOf("count" to 1, "word" to "on"),
      mapOf("count" to 2, "word" to "sea"),
      mapOf("count" to 1, "word" to "seashells"),
      mapOf("count" to 2, "word" to "seashore"),
      mapOf("count" to 4, "word" to "sells"),
      mapOf("count" to 4, "word" to "she"),
      mapOf("count" to 4, "word" to "shells"),
      mapOf("count" to 1, "word" to "shore"),
      mapOf("count" to 1, "word" to "so"),
      mapOf("count" to 1, "word" to "sure"),
      mapOf("count" to 1, "word" to "surely"),
      mapOf("count" to 3, "word" to "the")
  )
}