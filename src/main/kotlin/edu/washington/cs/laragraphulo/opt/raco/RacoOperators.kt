package edu.washington.cs.laragraphulo.opt.raco

import edu.washington.cs.laragraphulo.opt.Expr
import edu.washington.cs.laragraphulo.opt.Op
import edu.washington.cs.laragraphulo.opt.Name
import edu.washington.cs.laragraphulo.opt.Obj
import org.apache.accumulo.core.data.ArrayByteSequence


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
    val emitters: Op<List<Emitter>>,
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

class ParseRacoException(msg: String) : Exception(msg)

//@Suppress("UNCHECKED_CAST")
fun parsePTree(ptree: PTree) = PPT(ptree) //as Op<Relation>

@Suppress("UNCHECKED_CAST")
private fun PPT(ptree: PTree): Op<*> =
  when (ptree) {
    is PTree.PNode -> ptree.args.let { pa ->
      /** Assert arg length */
      fun AAL(elen: Int) { if (pa.size != elen) throw ParseRacoException("") }

      when (ptree.name) {
        "Dump" -> { AAL(1); Dump(PPT(pa[0]) as Op<Relation>) }
        "Apply" -> { AAL(2); Apply(PPT(pa[0]) as Op<List<Emitter>>, PPT(pa[1]) as Op<Relation>) }
        "FileScan" -> { AAL(4); FileScan(
            file = Obj((pa[0] as PTree.PString).str),
            format = Obj((pa[1] as PTree.PString).str),
            scheme = Obj(schemeToMap(pa[3] as PTree.PList)),
            options = Obj((pa[4] as PTree.PMap).map)) }
        else -> throw ParseRacoException("unexpected node: ${ptree.name}")
      }
    }
    is PTree.PList -> Obj(ptree.list)
    is PTree.PPair -> Obj(Pair(ptree.left, ptree.right))
    is PTree.PMap -> Obj(ptree.map)
    is PTree.PLong -> Obj(ptree.v)
    is PTree.PDouble -> Obj(ptree.v)
    is PTree.PString -> Obj(ptree.str)
  }

private fun schemeToMap(plist: PTree.PList) =
    plist.list.map {
      it as PTree.PPair
      it.left as PTree.PString
      it.right as PTree.PString
      it.left.str to it.right.str.let {
        val t = if (it.endsWith("_TYPE")) it.substring(0, it.length-5) else it
        val type = Type.valueOf(t)
        type
      }
    }.toMap()

