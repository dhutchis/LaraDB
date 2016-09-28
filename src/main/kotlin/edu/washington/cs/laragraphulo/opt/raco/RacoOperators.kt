package edu.washington.cs.laragraphulo.opt.raco

//import edu.washington.cs.laragraphulo.opt.Expr
import edu.washington.cs.laragraphulo.opt.Op
import edu.washington.cs.laragraphulo.opt.Name
import edu.washington.cs.laragraphulo.opt.Obj
import org.apache.accumulo.core.data.ArrayByteSequence
import sun.awt.image.PNGImageDecoder


enum class Type {
  LONG, BOOLEAN, DOUBLE, STRING, DATETIME, INT, FLOAT
}
typealias Scheme = List<Pair<Name,Type>>

typealias Tuple = Map<Name, ArrayByteSequence>
typealias Relation = List<Tuple>

typealias HashPartitioned = List<Name>


sealed class Expression<R>(args: List<Op<*>> = emptyList()): Op<R>(args) {
  constructor(vararg args: Op<*>): this(args.asList())

  sealed class Literal<R>(open val value: Any): Expression<R>() {
    data class StringLiteral(override val value: String): Literal<String>(value)
    data class BooleanLiteral(override val value: Boolean): Literal<Boolean>(value)
    data class NumericLiteral(override val value: Number): Literal<Number>(value)
  }

  data class NamedAttributeRef(val attributename: Name): Expression<Any?>()
  data class UnnamedAttributeRef(val position: Int, val debug_info: Any?): Expression<Any?>()

  data class PLUS<R>(val left: Op<Expression<R>>, val right: Op<Expression<R>>): Expression<R>(left, right)
}


sealed class Operator<R>(args: List<Op<*>> = emptyList()) : Op<R>(args) {
  constructor(vararg args: Op<*>): this(args.asList())
}

open class Join(
    condition: Op<Expression<Boolean>>,
    left: Op<Relation>,
    right: Op<Relation>
) : Operator<Relation>(condition, left, right)

class Dump(val input: Op<Relation>) : Operator<Unit>(input)

typealias Emitter = Pair<Name, Expression<*>>

class Apply(
    val emitters: Obj<List<Emitter>>,
    val input: Op<Relation>
) : Operator<Relation>(emitters, input)

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
) : Operator<Relation>(file, format, scheme, options)

class ParseRacoException(msg: String) : Exception(msg)

//@Suppress("UNCHECKED_CAST")
fun parsePTree(ptree: PTree) = PPT(ptree) //as Op<Relation>

@Suppress("UNCHECKED_CAST")
private fun PPT(ptree: PTree): Op<*> =
  when (ptree) {
    is PTree.PNode -> ptree.args.let { pa ->
      /** Assert arg length */
      fun AAL(elen: Int) { if (pa.size != elen) throw ParseRacoException("expected $elen arguments but got ${pa.size} arguments: $pa") }

      when (ptree.name) {
        "Dump" -> { AAL(1); Dump(PPT(pa[0]) as Op<Relation>) }
        "Apply" -> { AAL(2); Apply(
            PPT(pa[0]) as Obj<List<Emitter>>,
            PPT(pa[1]) as Op<Relation>) }
        "FileScan" -> { AAL(4); FileScan(
            file = Obj((pa[0] as PTree.PString).str),
            format = Obj((pa[1] as PTree.PString).str),
            scheme = Obj(schemeToMap(pa[2] as PTree.PNode)),
            options = Obj((pa[3] as PTree.PMap).map)) }
        "NamedAttributeRef" -> { AAL(1); Expression.NamedAttributeRef((pa[0] as PTree.PString).str) }
        else -> throw ParseRacoException("unexpected node: ${ptree.name}")
      }
    }
    is PTree.PList -> Obj(ptree.list.map { PPT(it) })
    is PTree.PPair -> Obj(Pair(PPT(ptree.left), PPT(ptree.right)))
    is PTree.PMap -> Obj(ptree.map.mapValues { PPT(it.value) })
    is PTree.PLong -> Obj(ptree.v)
    is PTree.PDouble -> Obj(ptree.v)
    is PTree.PString -> Obj(ptree.str)
  }

private fun schemeToMap(pscheme: PTree.PNode): Scheme {
  if (pscheme.name != "Scheme")
    throw ParseRacoException("expected a Scheme in arg 2 of FileScan")
  if (pscheme.args.size != 1)
    throw ParseRacoException("expected 1 argument to Scheme in arg 2 of FileScan, but have ${pscheme.args.size}: ${pscheme.args}")
  return schemeToMap0(pscheme.args[0] as PTree.PList)
}

private fun schemeToMap0(plist: PTree.PList): Scheme =
    plist.list.map {
      it as PTree.PPair
      it.left as PTree.PString
      it.right as PTree.PString
      it.left.str to it.right.str.let {
        val t = if (it.endsWith("_TYPE")) it.substring(0, it.length-5) else it
        val type = Type.valueOf(t)
        type
      }
    }

