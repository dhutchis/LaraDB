package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.Encode
import edu.washington.cs.laragraphulo.opt.raco.*
import org.apache.accumulo.core.data.ArrayByteSequence
import java.util.*
import java.util.concurrent.Callable


/**
 * Map position to attribute name. Concurs with the KeySchema on key attributes.
 * The purpose of this schema is to provide a position for value attributes.
 */
typealias PositionSchema = List<Name>








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
    re: RacoExpression,
    ep: KVAccessPath
): Expr<ArrayByteSequence> = when (re) {
//    is RacoExpression.Literal.StringLiteral -> Const(re.obj)
//    is RacoExpression.Literal.BooleanLiteral -> Const(re.obj)
//    is RacoExpression.Literal.DoubleLiteral -> Const(re.obj)
//    is RacoExpression.Literal.LongLiteral -> Const(re.obj)
  is RacoExpression.Literal<*> -> Const(re.toABS())

  is RacoExpression.NamedAttributeRef -> {
    convertAttributeRef(re.attributename, ep.allNames.indexOf(re.attributename), ep)
  }
  is RacoExpression.UnnamedAttributeRef -> {
    convertAttributeRef(ep.allNames[re.position], re.position, ep)
  }

  is RacoExpression.PLUS -> {
    val t = re.getType(ep)
    BinaryExpr<ArrayByteSequence,ArrayByteSequence,ArrayByteSequence>(racoExprToExpr(re.left, ep), racoExprToExpr(re.right, ep), { left: ArrayByteSequence, right: ArrayByteSequence ->
      fun <T> ArrayByteSequence.dec(ty: Type<T>) = ty.decode(this.backingArray, this.offset(), this.length())
      when (t) {
        Type.INT -> {
          t as Type.INT // compiler ought to be able to infer this; report bug
          t.encode(left.dec(t) + right.dec(t))
        }
        Type.INT_VARIABLE -> {
          t as Type.INT_VARIABLE
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
        Type.LONG_VARIABLE -> {
          t as Type.LONG_VARIABLE
          t.encode(left.dec(t) + right.dec(t))
        }
        Type.DOUBLE -> {
          t as Type.DOUBLE
          t.encode(left.dec(t) + right.dec(t))
        }
        Type.DOUBLE_VARIABLE -> {
          t as Type.DOUBLE_VARIABLE
          t.encode(left.dec(t) + right.dec(t))
        }
        Type.FLOAT -> {
          t as Type.FLOAT
          t.encode(left.dec(t) + right.dec(t))
        }
        Type.FLOAT_VARIABLE -> {
          t as Type.FLOAT
          t.encode(left.dec(t) + right.dec(t))
        }
        Type.STRING -> {
          t as Type.STRING
          t.encode(left.dec(t) + right.dec(t)) // replace with concatenating the byte[] representation, without decoding?
        }
        Type.UNKNOWN -> {
//            t as Type.UNKNOWN
          println("Warning! UNKNOWN type PLUS")
          val bs = ByteArray(left.length()+right.length())
          System.arraycopy(left.backingArray,left.offset(),bs,0,left.length())
          System.arraycopy(right.backingArray,right.offset(),bs,left.length(),right.length())
          bs
        }
      }.toABS()
      })
  }
}

fun ByteArray.toABS() = ArrayByteSequence(this)

/**
 * Decode <- eitherKeyOrFamilyOrFullValuePart
 */
fun <S> convertAttributeRef(name: String, allidx: Int, ep: S): Expr<ArrayByteSequence>
where S : KVSchema, S : TypeSchema {
  // __VIS
  // __TS
  // __FAMILY__
  val keyidx = ep.keyNames.indexOf(name)
  val lexicoder = ep.types[allidx] ?: Type.STRING
  return if (keyidx == -1) {
    // value attribute
    // use default reducer for now
    val reducer = defaultReducer //ep.reducingSchema.reducers[name]
    when {
      name == __FAMILY__ -> {
        TupleRef.RefFamily()
//        UnaryExpr<ArrayByteSequence, Any>(untyped, {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
      }
      name.endsWith(__VIS) -> {
        UnaryExpr<List<FullValue>,ArrayByteSequence>(TupleRef.RefVal(valName = name.substring(0,name.length-__VIS.length).toABS()), { it -> reducer(it).visibility })
//        UnaryExpr<ArrayByteSequence, Any>(untyped, {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
      }
      name.endsWith(__TS) -> {
        val ts = UnaryExpr<List<FullValue>,Long>(TupleRef.RefVal(valName = name.substring(0,name.length-__TS.length).toABS()), { it -> reducer(it).timestamp })
        UnaryExpr<Long,ArrayByteSequence>(ts, {it -> Type.LONG.encode(it).toABS()})
      }
      else -> {
        UnaryExpr<List<FullValue>,ArrayByteSequence>(TupleRef.RefVal(valName = name.toABS()), { it -> reducer(it).value})
//        UnaryExpr<ArrayByteSequence, Any>(untyped, {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
      }
    }
  } else {
    TupleRef.RefKey(keyNum = keyidx)
//    UnaryExpr<ArrayByteSequence, Any>(untyped, {it: ArrayByteSequence -> lexicoder.decode(it.backingArray, it.offset(), it.length())!!})
  }
}


fun exprToFullValueExpr(
    expr: Expr<ABS>,
    visRef: Expr<ABS> = Const(EMPTY),
    tsRef: Expr<Long> = Const(Long.MAX_VALUE)
): Expr<FullValue> = TernaryExpr(expr, visRef, tsRef) { value, vis, ts -> FullValue(value, vis, ts) }

fun nameToValueFirstVisRef(name: Name): Expr<ABS> =
    UnaryExpr(TupleRef.RefVal(valName = name.toABS())) { it.first().visibility }

fun nameToValueFirstTsRef(name: Name): Expr<Long> =
    UnaryExpr(TupleRef.RefVal(valName = name.toABS())) { it.first().timestamp }


fun racoToAccumulo(ro: RacoOperator, ep: SortedKeySchema): Pair<Op<*>, SortedAccessPath> {

  @Suppress("UNCHECKED_CAST")
  return when (ro) {


    is Apply -> {
      val (parent, parentSAP) = racoToAccumulo(ro.input, ep)
      parent as Op<TupleIterator>

      val emittersRaco0: List<Pair<Name, RacoExpression>> = ro.emitters
      val exprInfos0: List<Triple<Name, Expr<ArrayByteSequence>, Type<*>>> = emittersRaco0.map {
        Triple(it.first, racoExprToExpr(it.second, parentSAP), it.second.getType(parentSAP)) }
      // take out family expression if it does not exist
      val (exprInfos, famExpr) =  exprInfos0.find { it.first == __FAMILY__ }?.let {
        (exprInfos0 - it) to it.second
      } ?: exprInfos0 to TupleRef.RefFamily()
      val emittersRaco = emittersRaco0.find { it.first == __FAMILY__ }?.let { emittersRaco0 - it } ?: emittersRaco0
      val emittersType: List<Pair<Name, Type<*>>> = emittersRaco.map { it.first to it.second.getType(parentSAP) }


      val emittersScheme: AccessPath
      val keyExprs: List<Expr<ABS>>
      val valExprs: List< Pair<ABS,Expr<FullValue>> >
      // if __DAP__ and/or __LAP__ were present as emitted attributes, then they are accounted for in emittersScheme
      // if both were not present, then all attributes are in the DAP
      val (dap, lap, cap) = if (emittersType.any() { it.first == __DAP__ }) {
        emittersScheme = fromRacoScheme(emittersType)
        val dap = emittersScheme.dapNames.map { dapName -> exprInfos.find { it.first == dapName }!! }
        val lap = emittersScheme.lapNames.map { dapName -> exprInfos.find { it.first == dapName }!! }
        val cap = emittersScheme.valNames.map { dapName -> exprInfos.find { it.first == dapName }!! }
        Triple(dap,lap,cap)
      } else {
        // try to infer a good partitioning of attributes to dap/lap/cap
        val trip = exprInfos.fold(Triple(ArrayList<Triple<Name,Expr<ABS>,Type<*>>>(),ArrayList<Triple<Name,Expr<ABS>,Type<*>>>(),ArrayList<Triple<Name,Expr<ABS>,Type<*>>>())) {
          partitions, exprInfo ->
          val (dap, lap, cap) = partitions
          val (name, expr, type) = exprInfo
          // 1. if the name is present in the parent schema, then put it in the same place
          // 2. if the expression is constant, then put it in the value
          // 3. if the expression's inputs are all keys in the parent schema, then put it in the dap if the inputs are all from the dap, or put it in the lap otherwise
          // 4. put it in the cap
          when {
            parentSAP.dapNames.contains(name) -> dap
            parentSAP.lapNames.contains(name) -> lap
            parentSAP.valNames.contains(name) -> cap
            expr.inputs.isEmpty() -> cap
            expr.inputs.all { it is TupleRef.RefKey && it.keyNum < parentSAP.dapNames.size } -> dap
            expr.inputs.all { it is TupleRef.RefKey } -> lap
            else -> cap
          }.add(exprInfo)
          partitions
        }
        val (dap,lap,cap) = trip
        emittersScheme = AccessPath.of(dap.map { it.first }, lap.map { it.first }, cap.map { it.first }, (dap+lap+cap).map { it.third })
        trip
      }

      keyExprs = (dap+lap).map { it.second }
      valExprs = cap.map { it ->
        // If parent had the same value attribute defined, then pull the visibility and ts from it.
        // Otherwise use the Empty visibility and Long.MAX_VALUE timestamp
        if (parentSAP.valNames.contains(it.first)) {
//            if (parentSAP.types[parentSAP.valNames.indexOf(it.first)] == it.third)
          it.first.toABS() to exprToFullValueExpr(it.second, nameToValueFirstVisRef(it.first), nameToValueFirstTsRef(it.first))
//            else
//              it.first to exprToFullValueExpr(it.second)
        } else
          it.first.toABS() to exprToFullValueExpr(it.second)
      }

      // finally, calculate the sortedUpto
      // todo - find the longest prefix of keys that were copied from the parent. Take the minimum of that length and the parent's sortedUpto.
      val sortedUpto = 0
      val sap = emittersScheme.withSortedUpto(sortedUpto)

      OpApplyIterator(parent, keyExprs = keyExprs, famExpr = famExpr, valExprs = valExprs) to sap
    }

    is FileScan -> {
      // get the encoders; ensure we store properly; might need to implement the getExpressionProperties on other operators
      val scheme: List<Pair<Name, RacoType>> = ro.scheme
      val types: List<Pair<Name, Type<*>>> = scheme.map { it.first to racoTypeToType(it.second) }
      val encoders: List<Encode<String>?> = types.map { it.second.encodeFromString } // because we force ArrayByteSequence, all are encoded according to the String encoder

      val sk = ro.options["skip"]
      val skip = when (sk) {
        null -> 0
        is PTree.PLong -> sk.v.toInt()
        is PTree.PString -> sk.str.toInt()
        is PTree.PDouble -> sk.v.toInt()
        else -> throw RacoOperator.Companion.ParseRacoException("expected an int skip but got $sk")
      }

      // assume input is un-sorted when scanning from a file; add a FileScan option to assume some sorting later
      val sap: SortedAccessPath = fromRacoScheme(types).withSortedUpto(0)

      OpCSVScan(ro.file, encoders, types, skip = skip) to sap
    }

    else -> throw UnsupportedOperationException("nyi")
  }


  throw UnsupportedOperationException("nyi")
}



sealed class APReq {


  class SortedPrefixReq(
      val prefix: List<Name>
  ) : APReq()

  class SortedPrefixPermutationReq(
      val prefix: List<Name>
  ) : APReq()

}










// a RootRacoOp
//fun racoSequenceToExecutorTasks(sequence: RacoSequence): ExecutorTaskTree<*> {
//
//}

/** Accumulo tables only allow letters, numbers, and underscore. Convert ':' to '_'. */
fun sanitizeTableName(table: String): String = table.replace(':','_')
/** Accumulo tables only allow letters, numbers, and underscore. Convert ':' to '_'. */
fun sanitizeTableName(rk: RelationKey): String = "${rk.user}_${rk.program}_${rk.relation}"


//// inside a Sequence; probably a Store on top
//fun compileRacoFragment(
//    ro: RacoOperator<*>,
//    apReq: APReq,
//    accumuloConfig: AccumuloConfig): Pair<List<Callable<*>>, AccumuloOp> {
//
//  when (ro) {
//
//    is Store -> {
//      val tableName = sanitizeTableName(ro.relationKey)
//      // need to create the table we want to store in
//      val createTask = CreateTableTask(tableName, accumuloConfig)
//
//      val (executionList, compiledInput) = compileRacoFragment(ro.input, apReq, accumuloConfig)
//      OpRWI(compiledInput, tableName, accumuloConfig)
//    }
//
//  }
//
//  throw UnsupportedOperationException("nyi")
//}



