package edu.washington.cs.laragraphulo.opt.raco

import edu.washington.cs.laragraphulo.opt.Expr
import edu.washington.cs.laragraphulo.opt.Op
import edu.washington.cs.laragraphulo.opt.Name
import edu.washington.cs.laragraphulo.opt.Obj
import org.apache.accumulo.core.data.ArrayByteSequence
import java.io.*
import java.util.*


enum class Type {
  LONG, BOOLEAN, DOUBLE, STRING, DATETIME, INT, FLOAT
}
typealias Scheme = Map<Name,Type>

typealias Tuple = Map<Name, ArrayByteSequence>
typealias Relation = List<Tuple>

typealias HashPartitioned = List<Name>

sealed class Operator(args: List<Op<*>> = emptyList()) : Op<Relation>(args) {
  constructor(vararg args: Op<*>): this(args.asList())
}

open class Join(
    condition: Op<Expr<Boolean>>,
    left: Op<Relation>,
    right: Op<Relation>
) : Operator(condition, left, right)

class Dump(val input: Op<Relation>) : Operator(input)

typealias Emitter = Pair<Name, Expr<*>>

class Apply(
    val emitters: Obj<List<Emitter>>,
    val input: Op<Relation>
) : Operator(emitters, input)

/*
Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], Scan(RelationKey(
'public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000,
RepresentationProperties(frozenset([]), None, None))))

A = load("mock.csv", csv(schema(src:int,dst:int))); B = select src, dst from A; dump(B);

    Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))],
    FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))
 */

class FileScan(
    val file: Obj<String>,
    val format: Obj<String>,
    val scheme: Obj<Scheme>,
    val options: Obj<Map<String,Any>>
) : Operator(file, format, scheme, options)


/**
 * Python Tree. Parsed form of a Python repr string.
 */
sealed class PTree {
  data class PNode(val name: String, val args: List<PTree>) : PTree()
  data class PList(val args: List<PTree>) : PTree()
  data class PPair(val left: PTree, val right: PTree) : PTree()
  data class PMap(val map: Map<String, PTree>) : PTree()
  data class PLong(val v: Long) : PTree()
  data class PDouble(val v: Double) : PTree()
  data class PString(val str: String) : PTree()
}

/**
 * Parse a Python repr string into a [PTree]
 */
fun parseRaco(repr: Reader): PTree = parseRacoOne(PushbackReader(repr))

/**
 * NamedAttributeRef('dst')
 * @param fcc Optional first character
 */
private fun parseRacoOne(repr: PushbackReader, fcc: Char? = null): PTree {
  val fc = fcc ?: repr.readNonWhitespace()
  return when (fc) {
    '-', '.', in '0'..'9' -> {
      val num = repr.readNumber(fc)
      if (num.contains('.') || num.contains('e') || num.contains('E'))
        PTree.PDouble(num.toDouble())
      else
        PTree.PLong(num.toLong())
    }
    '\'', '"' -> PTree.PString(repr.readName(fc)) // string ends with matching ' or "
    '[' -> PTree.PList(parseRacoArgs(repr, ']'))
    '(' -> parseRacoOne(repr).let { // ignoring case of tuples with 3+ elements
      repr.readAssert(',')
      val it2 = parseRacoOne(repr)
      repr.readAssert(')')
      PTree.PPair(it, it2)
    }
    '{' -> PTree.PMap(parseRacoMap(repr))
    else -> repr.readName('(', fc).let { PTree.PNode(it, parseRacoArgs(repr, ')')) // read NAME(ARG1,ARG2,...,ARGN)
    }
  }
}

private fun PushbackReader.readNumber(firstDigit: Char? = null): String {
  val sb = StringBuilder()
  if (firstDigit != null) sb.append(firstDigit)
  loop@ while (true) {
    val r = this.read()
    if (r == -1) break
    val c = r.toChar()
    when (c) {
      '.', 'e', 'E', 'x', 'X', in '0'..'9' -> sb.append(c)
      else -> { this.unread(r); break@loop }
    }
  }
  return sb.toString()
}

/**
 * Positions the stream immediately after the end character
 * @param ec end character, exclusive
 * @param fc first character, optional and inclusive
 */
private fun Reader.readName(ec: Char, fc: Char? = null): String {
  val sbname = StringBuilder()
  if (fc != null) sbname.append(fc)
  while (true) {
    val r = this.read().assertNoEOS()
    if (r == ec) break
    sbname.append(r)
  }
  return sbname.toString()
}

private fun Int.assertNoEOS() = if (this == -1) throw IllegalArgumentException("repr ended early") else this.toChar()

private fun Reader.readNonWhitespace(): Char = this.discardWhitespace().assertNoEOS()

/** @return first non-whitespace character read, or -1 for end of stream */
private fun Reader.discardWhitespace(): Int {
  var fc: Int
  do {
    fc = this.read()
  } while (fc != -1 && Character.isWhitespace(fc))
  return fc
}

private fun Reader.readAssert(c: Char) {
  val r = this.discardWhitespace()
  if (r == -1 || r.toChar() != c)
    throw IllegalArgumentException(if (r == -1) "stream ended early but expected $c" else "read ${r.toChar()} but expected $c")
}

/** // [ excluded
 *  ('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))]
 *  // ] included
 *  @param ec either ']' or ')'
 */
private fun parseRacoArgs(repr: PushbackReader, ec: Char): List<PTree> {
  var t1: Char? = repr.readNonWhitespace()
  return if (t1 == ec) listOf()
  else {
    val arglist = ArrayList<PTree>()
    do {
      arglist.add(parseRacoOne(repr, t1))
      t1 = null
      val t = repr.readNonWhitespace() // additional args mean t is ','
    } while (t != ec)
    arglist
  }
}

/** // { excluded
 *  'a': 'b', 'skip': 0}
 *  // } included
 */
private fun parseRacoMap(repr: PushbackReader): Map<String,PTree> {
  var t1: Char? = repr.readNonWhitespace()
  return if (t1 == '}') mapOf()
  else {
    val map = HashMap<String,PTree>()
    do {
      val name = parseRacoOne(repr, t1)
      t1 = null
      if (name !is PTree.PString)
        throw IllegalArgumentException("expected a PString name in the map but got $name")
      repr.readAssert(':')
      map.put(name.str, parseRacoOne(repr))
      val t = repr.readNonWhitespace() // additional mappings mean t is ','
    } while (t != '}')
    map
  }
}