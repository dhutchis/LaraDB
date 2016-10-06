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
              "Store" -> { AAL(2); Store(PPT(pa[0]) as RelationKey, PPT(pa[1]) as RacoOperator<Relation>) }
              "Dump" -> { AAL(1); Dump(PPT(pa[0]) as Op<Relation>) }
              "Apply" -> { AAL(2); Apply(
                  PPT(pa[0]) as Obj<List<Emitter>>,
                  PPT(pa[1]) as RacoOperator<List<Map<Name, ArrayByteSequence>>>) }
              "FileScan" -> { AAL(4); FileScan(
                  file = Obj((pa[0] as PTree.PString).str),
                  format = Obj((pa[1] as PTree.PString).str),
                  scheme = Obj(schemeToMap(pa[2] as PTree.PNode)),
                  options = Obj((pa[3] as PTree.PMap).map/*.mapValues { it.value }*/)) }
              "NamedAttributeRef" -> { AAL(1); RacoExpression.NamedAttributeRef(Obj((pa[0] as PTree.PString).str)) }
              "Scan" -> { AAL(4); Scan(
                  relationKey = PPT(pa[0]) as RelationKey,
                  scheme = Obj(schemeToMap(pa[1] as PTree.PNode)),
                  cardinality = Obj((pa[2] as PTree.PLong).v),
                  partitioning = PPT(pa[3]) as RepresentationProperties
              ) }
              "RelationKey" -> { AAL(3); RelationKey(
                  user = Obj((pa[0] as PTree.PString).str),
                  program = Obj((pa[1] as PTree.PString).str),
                  relation = Obj((pa[2] as PTree.PString).str)
              ) }
              "RepresentationProperties" -> { AAL(3); RepresentationProperties(
                  hashPartition = (PPT(pa[0]) as Obj<List<String>>),
                  sorted = PPT(pa[1]) as Obj<List<String>>,
                  grouped = PPT(pa[2]) as Obj<List<String>>
              ) }
              "frozenset" -> {AAL(1); (PPT(pa[0]) as Obj<List<Op<*>>>)().map { (it as Obj<String>)() }.let { Obj(it) } as Obj<List<String>> }
              else -> throw ParseRacoException("unexpected node: ${ptree.name}")
            }
          }
          is PTree.PList -> Obj(ptree.list.map { PPT(it) })
          is PTree.PPair -> Obj(Pair(PPT(ptree.left), PPT(ptree.right)))
          is PTree.PMap -> Obj(ptree.map.mapValues { PPT(it.value) })
          is PTree.PLong -> Obj(ptree.v)
          is PTree.PDouble -> Obj(ptree.v)
          is PTree.PString -> Obj(ptree.str)
          PTree.PNone -> Obj(listOf<String>())
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

sealed class RacoTopLevel(args: List<Op<*>> = emptyList()) : Op<Unit>(args) {
  constructor(vararg args: Op<*>) : this(args.asList())

  class Sequence(val input: RacoOperator<*>) : RacoTopLevel(input)
}

data class Store(val relationKey: RelationKey, val input: RacoOperator<Relation>) : RacoOperator<Relation>(relationKey, input)

//open class Join(
//    condition: Op<RacoExpression<Boolean>>,
//    left: Op<Relation>,
//    right: Op<Relation>
//) : RacoOperator<Relation>(condition, left, right)

data class Dump(val input: Op<Relation>) : RacoOperator<Unit>(input)

typealias Emitter = Pair<Name, RacoExpression<*>>

data class Apply(
    val emitters: Obj<List<Emitter>>,
    val input: RacoOperator<Relation>
) : RacoOperator<Relation>(emitters, input)

/*
Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], Scan(RelationKey(
'public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000,
RepresentationProperties(frozenset([]), None, None))))

A = load("mock.csv", csv(schema(src:int,dst:int))); B = select src, dst from A; dump(B);

    Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))],
    FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))
 */

data class FileScan(
    val file: Obj<String>,
    val format: Obj<String>,
    val scheme: Obj<Scheme>,
    val options: Obj<Map<String,PTree>>
) : RacoOperator<Relation>(file, format, scheme, options)

data class RelationKey(
    val user: Obj<String>,
    val program: Obj<String>,
    val relation: Obj<String>
) : RacoOperator<Relation>(user, program, relation)

data class RepresentationProperties(
    val hashPartition: Obj<List<String>>,
    val sorted: Obj<List<String>>,
    val grouped: Obj<List<String>>
) : RacoOperator<Relation>(hashPartition, sorted, grouped)

data class Scan(
    val relationKey: RelationKey,
    val scheme: Obj<Scheme>,
    val cardinality: Obj<Long>,
    val partitioning: RepresentationProperties
) : RacoOperator<Relation>(relationKey, scheme, cardinality, partitioning)


