package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.Encode
import edu.washington.cs.laragraphulo.opt.raco.*
import org.apache.accumulo.core.data.ArrayByteSequence


/**
 * Map position to attribute name. Concurs with the KeySchema on key attributes.
 * The purpose of this schema is to provide a position for value attributes.
 */
typealias PositionSchema = List<Name>

data class ExpressionProperties(
    val encodingSchema: EncodingSchema,
    val reducingSchema: ReducingSchema,
    val keySchema: KeySchema,
    val positionSchema: PositionSchema
)






fun racoTypeToType(racoType: RacoType): Type<*> = when(racoType) {
  RacoType.LONG -> Type.LONG
  RacoType.BOOLEAN -> Type.BOOLEAN
  RacoType.DOUBLE -> Type.DOUBLE
  RacoType.STRING -> Type.STRING
  RacoType.DATETIME -> throw UnsupportedOperationException("DATETIME not supported yet")
  RacoType.INT -> Type.INT
  RacoType.FLOAT -> Type.FLOAT
}

const val __VIS = "__VIS"
const val __TS = "__TS"
const val __FAMILY__ = "__FAMILY__"
fun String.toABS() = this.toByteArray().let { ArrayByteSequence(it, 0, it.size) }

val defaultReducer = { list: List<FullValue> -> when (list.size) {
  0 -> throw RuntimeException("no default value provided but the value is missing")
  1 -> list[0]
  else -> throw RuntimeException("did not expect > 1 values: $list")
} }


/**
 * @param positionSchema The positions associated with each attribute, in the lens of raco. keySchema only includes the keys.
 */
fun racoExprToExpr(
    re: RacoExpression<*>,
    ep: ExpressionProperties
): Expr<ArrayByteSequence> {
  return when (re) {
//    is RacoExpression.Literal.StringLiteral -> Const(re.obj)
//    is RacoExpression.Literal.BooleanLiteral -> Const(re.obj)
//    is RacoExpression.Literal.DoubleLiteral -> Const(re.obj)
//    is RacoExpression.Literal.LongLiteral -> Const(re.obj)
    is RacoExpression.Literal -> Const(Obj(re.toABS()))

    is RacoExpression.NamedAttributeRef -> {
      convertAttributeRef(re.attributename(), ep)
    }
    is RacoExpression.UnnamedAttributeRef -> {
      convertAttributeRef(ep.positionSchema[re.position()], ep)
    }

    is RacoExpression.PLUS<*> -> {
      val t = re.getType(ep)
      BinaryExpr<ArrayByteSequence,ArrayByteSequence,ArrayByteSequence>(racoExprToExpr(re.left, ep), racoExprToExpr(re.right, ep), Obj() { left: ArrayByteSequence, right: ArrayByteSequence ->
        fun <T> ArrayByteSequence.dec(ty: Type<T>) = ty.decode(this.backingArray, this.offset(), this.length())
        when (t) {
          Type.INT -> {
            t as Type.INT // compiler ought to be able to infer this; report bug
            t.encode(left.dec(t) + right.dec(t))
          }
          Type.BOOLEAN -> {
            t as Type.BOOLEAN
            t.encode(left.dec(t) || right.dec(t))
          }
          Type.LONG -> {
            t as Type.LONG
            t.encode(left.dec(t) + right.dec(t))
          }
          Type.DOUBLE -> {
            t as Type.DOUBLE
            t.encode(left.dec(t) + right.dec(t))
          }
          Type.FLOAT -> {
            t as Type.FLOAT
            t.encode(left.dec(t) + right.dec(t))
          }
          Type.STRING -> {
            t as Type.STRING
            t.encode(left.dec(t) + right.dec(t))
          }
        }.toABS()
        })
    }
  }
}

fun ByteArray.toABS() = ArrayByteSequence(this)

/**
 * Decode <- eitherKeyOrFamilyOrFullValuePart
 */
fun convertAttributeRef(name: String, ep: ExpressionProperties): Expr<ArrayByteSequence> {
  // __VIS
  // __TS
  // __FAMILY__
  val idx = ep.keySchema.keyNames.indexOf(name)
  val lexicoder = ep.encodingSchema.encodings[name] ?: Type.STRING
  return if (idx == -1) {
    // value attribute
    val reducer = ep.reducingSchema.reducers[name] ?: defaultReducer
    when {
      name == __FAMILY__ -> {
        TupleRef.RefFamily()
//        UnaryExpr<ArrayByteSequence, Any>(untyped, Obj() {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
      }
      name.endsWith(__VIS) -> {
        UnaryExpr<List<FullValue>,ArrayByteSequence>(TupleRef.RefVal(valName = Obj(name.substring(0,name.length-__VIS.length).toABS())), Obj() { it -> reducer(it).visibility })
//        UnaryExpr<ArrayByteSequence, Any>(untyped, Obj() {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
      }
      name.endsWith(__TS) -> {
        val ts = UnaryExpr<List<FullValue>,Long>(TupleRef.RefVal(valName = Obj(name.substring(0,name.length-__TS.length).toABS())), Obj() {it -> reducer(it).timestamp })
        UnaryExpr<Long,ArrayByteSequence>(ts, Obj() {it -> Type.LONG.encode(it).toABS()})
      }
      else -> {
        UnaryExpr<List<FullValue>,ArrayByteSequence>(TupleRef.RefVal(valName = Obj(name.toABS())), Obj() {it -> reducer(it).value})
//        UnaryExpr<ArrayByteSequence, Any>(untyped, Obj() {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
      }
    }
  } else {
    TupleRef.RefKey(keyNum = Obj(idx))
//    UnaryExpr<ArrayByteSequence, Any>(untyped, Obj() {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
  }
}


//fun extract


fun racoToAccumulo(ro: RacoOperator<*>, ep: ExpressionProperties): AccumuloOp {

  val x = when (ro) {


    is Apply -> {
      // todo make an ApplyOp

      val parent = racoToAccumulo(ro.input, ep)
      val emitters: List<Pair<Name, RacoExpression<*>>> = ro.emitters()


      OpApplyIterator(parent, keyExprs = , famExpr = , valExprs = , keySchema = )

      throw UnsupportedOperationException("nyi")
    }

    is FileScan -> {
      // get the encoders; ensure we store properly; might need to implement the getExpressionProperties on other operators
      val scheme: List<Pair<Name, RacoType>> = ro.scheme.obj
      val types: List<Pair<Name, Type<*>>> = scheme.map { it.first to racoTypeToType(it.second) }
      val encoders: Obj<List<Encode<String>?>> = Obj(types.map { it.second.encodeFromString }) // because we force ArrayByteSequence, all are encoded according to the String encoder
      val names = Obj(types)

      val sk = ro.options()["skip"]
      val skip = Obj(when (sk) {
        null -> null
        is PTree.PLong -> sk.v.toInt()
        is PTree.PString -> sk.str.toInt()
        is PTree.PDouble -> sk.v.toInt()
        else -> throw RacoOperator.Companion.ParseRacoException("expected an int skip but got $sk")
      } ?: 0)
      OpCSVScan(ro.file, encoders, names, skip = skip)
    }


  }



}



sealed class APRequirement










// a RootRacoOp
//fun racoSequenceToExecutorTasks(sequence: RacoSequence): ExecutorTaskTree<*> {
//
//}


// inside a Sequence; probably a Store on top
fun compileRacoFragment(ro: RacoOperator<*>)



