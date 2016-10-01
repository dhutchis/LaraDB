package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.LexicoderPlus
import org.apache.accumulo.core.client.lexicoder.*

sealed class Type<T> : LexicoderPlus<T> {
  object INT : Type<Int>() {
    val lex = IntegerLexicoder()
    override fun encode(v: Int): ByteArray = lex.encode(v)
    override fun decode(b: ByteArray, offset: Int, len: Int): Int = lex.decode(b, offset, len)
  }
  object LONG : Type<Long>() {
    val lex = LongLexicoder()
    override fun decode(b: ByteArray, offset: Int, len: Int): Long = lex.decode(b, offset, len)
    override fun encode(v: Long): ByteArray = lex.encode(v)
  }
  object BOOLEAN : Type<Boolean>() {
    const val ZERO: Byte = 0
    override fun decode(b: ByteArray, offset: Int, len: Int): Boolean = if (b[offset] == ZERO) false else true
    val FALSE = byteArrayOf(0)
    val TRUE = byteArrayOf(1)
    override fun encode(v: Boolean): ByteArray = if (v) TRUE else FALSE
  }
  object DOUBLE : Type<Double>() {
    val lex = DoubleLexicoder()
    override fun decode(b: ByteArray, offset: Int, len: Int): Double = lex.decode(b, offset, len)
    override fun encode(v: Double): ByteArray = lex.encode(v)
  }
  object STRING : Type<String>() {
    val lex = StringLexicoder()
    override fun decode(b: ByteArray, offset: Int, len: Int): String = lex.decode(b, offset, len)
    override fun encode(v: String): ByteArray = lex.encode(v)
  }
//  object DATETIME : Type<DateTime>() {
//    val lex = DateTimeLexicoder()
//    override fun decode(b: ByteArray, offset: Int, len: Int): DateTime = lex.decode(b, offset, len)
//    override fun encode(v: DateTime?): ByteArray = lex.encode(v)
//  }
  object FLOAT : Type<Float>() {
    val lex = FloatLexicoder()
    override fun decode(b: ByteArray, offset: Int, len: Int): Float = lex.decode(b, offset, len)
    override fun encode(v: Float): ByteArray = lex.encode(v)
  }
//  object BYTE_ARRAY : Type<ByteArray>() {
//
//  }
}

