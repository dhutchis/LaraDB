package edu.washington.cs.laragraphulo.opt.raco

//import edu.washington.cs.laragraphulo.opt.Expr
import com.google.common.collect.ImmutableList
import edu.washington.cs.laragraphulo.opt.*
import org.apache.accumulo.core.data.ArrayByteSequence


enum class RacoType {
  LONG, BOOLEAN, DOUBLE, STRING, DATETIME, INT, FLOAT
}
typealias Scheme = List<Pair<Name, RacoType>>

typealias Tuple = Map<Name, ArrayByteSequence>
typealias Relation = List<Tuple>

typealias HashPartitioned = List<Name>

sealed class RacoExpression(args: List<Op<*>> = emptyList()): Op<Unit>(args) {
  constructor(vararg args: Op<*>): this(ImmutableList.copyOf(args))

  override val unbound: List<Arg<*>> = emptyList()
  override fun invoke(reqs: List<*>) {}

  abstract fun <P> getType(props: P): Type<*> where P : NameSchema, P : TypeSchema

  sealed class Literal<out R>(open val obj: R): RacoExpression(obj.toObj()){
    abstract fun toABS(): ArrayByteSequence
    data class StringLiteral(override val obj: String): Literal<String>(obj) {
      override fun toABS(): ArrayByteSequence = Type.STRING.encode(obj).toABS()
      override fun <P> getType(props: P) where P : NameSchema, P : TypeSchema = Type.STRING
    }
    data class BooleanLiteral(override val obj: Boolean): Literal<Boolean>(obj) {
      override fun toABS(): ArrayByteSequence = Type.BOOLEAN.encode(obj).toABS()
      override fun <P> getType(props: P) where P : NameSchema, P : TypeSchema = Type.BOOLEAN
    }
//    data class NumericLiteral(override val obj: Number): Literal<Number>(obj)
    data class DoubleLiteral(override val obj: Double): Literal<Double>(obj) {
      override fun toABS(): ArrayByteSequence = Type.DOUBLE.encode(obj).toABS()
      override fun <P> getType(props: P) where P : NameSchema, P : TypeSchema = Type.DOUBLE
    }
    data class LongLiteral(override val obj: Long): Literal<Long>(obj) {
      override fun toABS(): ArrayByteSequence = Type.LONG.encode(obj).toABS()
      override fun <P> getType(props: P) where P : NameSchema, P : TypeSchema = Type.LONG
    }
  }

  data class NamedAttributeRef(val attributename: Name): RacoExpression(attributename.toObj()) {
    override fun <P> getType(props: P): Type<*> where P : NameSchema, P : TypeSchema {
      val enc = props.types[props.allNames.indexOf(attributename)]
      return enc
    }
  }
  data class UnnamedAttributeRef(val position: Int, val debug_info: Any?): RacoExpression(position.toObj(), debug_info.toObj()) {
    override fun <P> getType(props: P): Type<*> where P : NameSchema, P : TypeSchema {
      val enc = props.types[position]
      return enc
    }
  }

  data class PLUS(val left: RacoExpression, val right: RacoExpression): RacoExpression(left, right) {
    override fun <P> getType(props: P): Type<*> where P : NameSchema, P : TypeSchema {
      val t = left.getType(props)
      if (t != right.getType(props))
        throw RuntimeException("types mismatch: $t and ${right.getType(props)}. This might be fixable by type upcasting")
      return t
    }
  }

  data class DIVIDE(val left: RacoExpression, val right: RacoExpression): RacoExpression(left, right) {
    override fun <P> getType(props: P): Type<*> where P : NameSchema, P : TypeSchema {
      val l = left.getType(props)
      val r = right.getType(props)
      return when (l) {
        r -> l
        Type.INT -> {
          when (r) {
            Type.DOUBLE -> Type.DOUBLE
            else -> throw UnsupportedOperationException("did not implement other type conversions when l is an INT for $r")
          }
        }
        else -> throw UnsupportedOperationException("types mismatch: $l and ${right.getType(props)}. This might be fixable by type upcasting")
      }
    }
  }

  data class GT(val left: RacoExpression, val right: RacoExpression): RacoExpression(left, right) {
    override fun <P> getType(props: P): Type<*> where P : NameSchema, P : TypeSchema {
      val t = left.getType(props)
      if (t != right.getType(props))
        throw RuntimeException("types mismatch: $t and ${right.getType(props)}. This might be fixable by type upcasting")
      return Type.BOOLEAN
    }
  }
}


sealed class RacoOperator(args: List<Op<*>> = emptyList()) : Op<Unit>(args) {
  constructor(vararg args: Op<*>): this(ImmutableList.copyOf(args))

//  abstract val scheme: Scheme

  override val unbound: List<Arg<*>> = emptyList()
  override fun invoke(reqs: List<*>) {}

  companion object {
    class ParseRacoException(msg: String) : Exception(msg)

    @Suppress("UNCHECKED_CAST")
    fun parsePTreeToRacoTree(ptree: PTree) = PPT(ptree) as RacoOperator

    @Suppress("UNCHECKED_CAST", "IMPLICIT_CAST_TO_ANY")
    private fun PPT(ptree: PTree): Any =
        when (ptree) {
          is PTree.PNode -> ptree.args.let { pa ->
            /** Assert arg length */
            fun AAL(elen: Int) { if (pa.size != elen) throw ParseRacoException("expected $elen arguments but got ${pa.size} arguments: $pa") }

            when (ptree.name) {
              "Store" -> { AAL(2); Store(PPT(pa[0]) as RelationKey, PPT(pa[1]) as RacoOperator) }
              "Dump" -> { AAL(1); Dump(PPT(pa[0]) as Op<Relation>) }
              "Apply" -> { AAL(2); Apply(
                  PPT(pa[0]) as List<Emitter>,
                  PPT(pa[1]) as RacoOperator
              ) }
              "FileScan" -> { AAL(4); FileScan(
                  file = (pa[0] as PTree.PString).str,
                  format = ((pa[1] as PTree.PString).str),
                  scheme = (schemeToMap(pa[2] as PTree.PNode)),
                  options = ((pa[3] as PTree.PMap).map/*.mapValues { it.value }*/)
              ) }
              "Select" -> { AAL(2); Select(
                  PPT(pa[0]) as RacoExpression,
                  PPT(pa[1]) as RacoOperator
              ) }
              "NamedAttributeRef" -> { AAL(1); RacoExpression.NamedAttributeRef(((pa[0] as PTree.PString).str)) }
              "UnnamedAttributeRef" -> { AAL(2); val dbg = pa[1]; if (dbg != PTree.PNone) throw ParseRacoException("no support for non-None debug_info")
                RacoExpression.UnnamedAttributeRef(((pa[0] as PTree.PLong).v.toInt()),
                  debug_info = null
                ) }
              "PLUS" -> { AAL(2); RacoExpression.PLUS(PPT(pa[0]) as RacoExpression, PPT(pa[1]) as RacoExpression)}
              "DIVIDE" -> { AAL(2); RacoExpression.DIVIDE(PPT(pa[0]) as RacoExpression, PPT(pa[1]) as RacoExpression)}
              "GT" -> { AAL(2); RacoExpression.GT(PPT(pa[0]) as RacoExpression, PPT(pa[1]) as RacoExpression)}
              "Scan" -> { AAL(4); Scan(
                  relationKey = PPT(pa[0]) as RelationKey,
                  scheme = (schemeToMap(pa[1] as PTree.PNode)),
                  cardinality = ((pa[2] as PTree.PLong).v),
                  partitioning = PPT(pa[3]) as RepresentationProperties
              ) }
              "RelationKey" -> { AAL(3); RelationKey(
                  user = ((pa[0] as PTree.PString).str),
                  program = ((pa[1] as PTree.PString).str),
                  relation = ((pa[2] as PTree.PString).str)
              ) }
              "RepresentationProperties" -> { AAL(3); RepresentationProperties(
                  hashPartition = (PPT(pa[0]) as List<String>),
                  sorted = PPT(pa[1]) as List<String>,
                  grouped = PPT(pa[2]) as List<String>
              ) }
              "frozenset" -> {AAL(1); (PPT(pa[0]) as List<String>) }
              "NumericLiteral" -> {AAL(1)
                val lit = PPT(pa[0])
                when (lit) {
                  is Long -> RacoExpression.Literal.LongLiteral(lit)
                  is Double -> RacoExpression.Literal.DoubleLiteral(lit)
                  else -> throw ParseRacoException("unexpected inside NumbericLiteral: $lit")
                }
              }
              "StringLiteral" -> {AAL(1)
                RacoExpression.Literal.StringLiteral(PPT(pa[0]) as String)
              }
              else -> throw ParseRacoException("unexpected node: ${ptree.name}")
            }
          }
          is PTree.PList ->   ptree.list.map { PPT(it) }
          is PTree.PPair ->   Pair(PPT(ptree.left), PPT(ptree.right))
          is PTree.PMap ->    ptree.map.mapValues { PPT(it.value) }
          is PTree.PLong ->   ptree.v
          is PTree.PDouble -> ptree.v
          is PTree.PString -> ptree.str
          PTree.PNone -> listOf<String>()
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

//sealed class RacoTopLevel(args: List<Op<*>> = emptyList()) : Op<Unit>(args) {
//  constructor(vararg args: Op<*>) : this(ImmutableList.copyOf(args))
//
//  class Sequence(val input: RacoOperator) : RacoTopLevel(input)
//}

data class Store(val relationKey: RelationKey, val input: RacoOperator) : RacoOperator(relationKey, input)

//open class Join(
//    condition: Op<RacoExpression<Boolean>>,
//    left: Op<Relation>,
//    right: Op<Relation>
//) : RacoOperator(condition, left, right)

data class Dump(val input: Op<Relation>) : RacoOperator(input)

typealias Emitter = Pair<Name, RacoExpression>

data class Apply(
    val emitters: List<Emitter>,
    val input: RacoOperator
) : RacoOperator(emitters.toObj(), input)

data class Select(
    val condition: RacoExpression,
    val input: RacoOperator
) : RacoOperator(condition, input)

/*
Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], Scan(RelationKey(
'public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000,
RepresentationProperties(frozenset([]), None, None))))

A = load("mock.csv", csv(schema(src:int,dst:int))); B = select src, dst from A; dump(B);

    Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))],
    FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))
 */

data class FileScan(
    val file: String,
    val format: String,
    val scheme: Scheme,
    val options: Map<String, PTree>
) : RacoOperator(file.toObj(), format.toObj(), scheme.toObj(), options.toObj())

data class RelationKey(
    val user: String,
    val program: String,
    val relation: String
) : RacoOperator(user.toObj(), program.toObj(), relation.toObj())

data class RepresentationProperties(
    val hashPartition: List<String>,
    val sorted: List<String>,
    val grouped: List<String>
) : RacoOperator(hashPartition.toObj(), sorted.toObj(), grouped.toObj())

data class Scan(
    val relationKey: RelationKey,
    val scheme: Scheme,
    val cardinality: Long,
    val partitioning: RepresentationProperties
) : RacoOperator(relationKey, scheme.toObj(), cardinality.toObj(), partitioning)


