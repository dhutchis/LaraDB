package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder

sealed class Type<T> : LexicoderPlus<T> {

//  abstract fun

  object INT : Type<Int>() {
    val lex = IntegerLexicoder()

    override fun encode(v: Int): ByteArray {
      return lex.encode(v)
    }

    override fun decode(b: ByteArray, offset: Int, len: Int): Int {
      return lex.decode(b, offset, len)
    }
  }

//  object INT : Type<Int> by IntegerLexicoder()

}