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

