package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.opt.raco.RacoExpression
import edu.washington.cs.laragraphulo.opt.raco.RacoOperator
import edu.washington.cs.laragraphulo.opt.raco.RacoType
import org.apache.accumulo.core.data.ArrayByteSequence


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
const val __FAMILY__ = "__FAMILY"
fun String.toABS() = this.toByteArray().let { ArrayByteSequence(it, 0, it.size) }

val defaultReducer = { list: List<FullValue> -> when (list.size) {
  0 -> throw RuntimeException("no default value provided but the value is missing")
  1 -> list[0]
  else -> throw RuntimeException("did not expect > 1 values: $list")
} }

data class ExpressionProperties(
    val encodingSchema: EncodingSchema,
    val reducingSchema: ReducingSchema,
    val keySchema: KeySchema,
    val positionSchema: List<Name>
)

/**
 * @param positionSchema The positions associated with each attribute, in the lens of raco. keySchema only includes the keys.
 */
fun racoExprToExpr(
    re: RacoExpression<*>,
    encodingSchema: EncodingSchema,
    reducingSchema: ReducingSchema,
    keySchema: KeySchema,
    positionSchema: List<Name>
): Expr<*> {
  val const: Expr<ArrayByteSequence> = when (re) {
//    is RacoExpression.Literal.StringLiteral -> Const(re.obj)
//    is RacoExpression.Literal.BooleanLiteral -> Const(re.obj)
//    is RacoExpression.Literal.DoubleLiteral -> Const(re.obj)
//    is RacoExpression.Literal.LongLiteral -> Const(re.obj)
    is RacoExpression.Literal -> Const(Obj(re.toABS()))

    is RacoExpression.NamedAttributeRef -> {
      convertAttributeRef(re.attributename(), encodingSchema, reducingSchema, keySchema)
    }
    is RacoExpression.UnnamedAttributeRef -> {
      convertAttributeRef(positionSchema[re.position()], encodingSchema, reducingSchema, keySchema)
    }

    is RacoExpression.PLUS<*> -> {

    }
  }
}

fun ByteArray.toABS() = ArrayByteSequence(this)

/**
 * Decode <- eitherKeyOrFamilyOrFullValuePart
 */
fun convertAttributeRef(name: String, encodingSchema: EncodingSchema, reducingSchema: ReducingSchema, keySchema: KeySchema): Expr<ArrayByteSequence> {
  // __VIS
  // __TS
  // __FAMILY__
  val idx = keySchema.keyNames.indexOf(name)
  val lexicoder = encodingSchema.encodings[name] ?: Type.STRING
  return if (idx == -1) {
    // value attribute
    val reducer = reducingSchema.reducers[name] ?: defaultReducer
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


fun racoToAccumulo(racoOp: RacoOperator<*>) {

}







