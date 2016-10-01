package edu.washington.cs.laragraphulo.opt.raco

//import edu.washington.cs.laragraphulo.opt.Expr
import edu.washington.cs.laragraphulo.opt.*
import org.apache.accumulo.core.data.ArrayByteSequence


enum class RacoType {
  LONG, BOOLEAN, DOUBLE, STRING, DATETIME, INT, FLOAT
}
typealias Scheme = List<Pair<Name, RacoType>>

typealias Tuple = Map<Name, ArrayByteSequence>
typealias Relation = List<Tuple>

typealias HashPartitioned = List<Name>

// todo: consider deleting the <R> - it messes everything up because we can't statically know it
sealed class RacoExpression<R>(args: List<Op<*>> = emptyList()): Op<R>(args) {
  constructor(vararg args: Op<*>): this(args.asList())

  abstract fun getType(props: ExpressionProperties): Type<R>

  sealed class Literal<R>(open val obj: Obj<R>): RacoExpression<R>(obj) {
    abstract fun toABS(): ArrayByteSequence
    data class StringLiteral(override val obj: Obj<String>): Literal<String>(obj) {
      override fun toABS(): ArrayByteSequence = Type.STRING.encode(obj()).toABS()
      override fun getType(props: ExpressionProperties) = Type.STRING
    }
    data class BooleanLiteral(override val obj: Obj<Boolean>): Literal<Boolean>(obj) {
      override fun toABS(): ArrayByteSequence = Type.BOOLEAN.encode(obj()).toABS()
      override fun getType(props: ExpressionProperties) = Type.BOOLEAN
    }
//    data class NumericLiteral(override val obj: Obj<Number>): Literal<Number>(obj)
    data class DoubleLiteral(override val obj: Obj<Double>): Literal<Double>(obj) {
      override fun toABS(): ArrayByteSequence = Type.DOUBLE.encode(obj()).toABS()
      override fun getType(props: ExpressionProperties) = Type.DOUBLE
    }
    data class LongLiteral(override val obj: Obj<Long>): Literal<Long>(obj) {
      override fun toABS(): ArrayByteSequence = Type.LONG.encode(obj()).toABS()
      override fun getType(props: ExpressionProperties) = Type.LONG
    }
  }

  data class NamedAttributeRef(val attributename: Obj<Name>): RacoExpression<Any?>(attributename) {
    override fun getType(props: ExpressionProperties): Type<Any?> {
      val enc = props.encodingSchema.encodings[attributename()] ?: throw UnsupportedOperationException("no encoder / type information for attribute $attributename")
      return enc as Type<Any?>
    }
  }
  data class UnnamedAttributeRef(val position: Obj<Int>, val debug_info: Obj<Any?>): RacoExpression<Any?>(position, debug_info) {
    override fun getType(props: ExpressionProperties): Type<Any?> {
      val enc = props.encodingSchema.encodings[props.positionSchema[position()]] ?: throw UnsupportedOperationException("no encoder / type information for attribute #$position")
      return enc as Type<Any?>
    }
  }

  data class PLUS<R>(val left: RacoExpression<R>, val right: RacoExpression<R>): RacoExpression<R>(left, right) {
    override fun getType(props: ExpressionProperties): Type<R> {
      val t = left.getType(props)
      if (t != right.getType(props))
        throw RuntimeException("types mismatch: $t and ${right.getType(props)}. This might be fixable by type upcasting")
      return t
    }
  }
}


sealed class RacoOperator<R>(args: List<Op<*>> = emptyList()) : Op<R>(args) {
  constructor(vararg args: Op<*>): this(args.asList())

  companion object {
    class ParseRacoException(msg: String) : Exception(msg)

    @Suppress("UNCHECKED_CAST")
    fun parsePTreeToRacoTree(ptree: PTree) = PPT(ptree) as RacoOperator<*>

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
              "NamedAttributeRef" -> { AAL(1); RacoExpression.NamedAttributeRef(Obj((pa[0] as PTree.PString).str)) }
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
            val type = RacoType.valueOf(t)
            type
          }
        }


  }
}

//open class Join(
//    condition: Op<RacoExpression<Boolean>>,
//    left: Op<Relation>,
//    right: Op<Relation>
//) : RacoOperator<Relation>(condition, left, right)

class Dump(val input: Op<Relation>) : RacoOperator<Unit>(input)

typealias Emitter = Pair<Name, RacoExpression<*>>

class Apply(
    val emitters: Obj<List<Emitter>>,
    val input: Op<Relation>
) : RacoOperator<Relation>(emitters, input)

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
) : RacoOperator<Relation>(file, format, scheme, options)

