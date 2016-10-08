package edu.washington.cs.laragraphulo.opt

import edu.washington.cs.laragraphulo.Encode
import edu.washington.cs.laragraphulo.opt.raco.*
import org.apache.accumulo.core.data.ArrayByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import java.io.ObjectOutputStream
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
 *
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
    val reducer = defaultReducer //req.reducingSchema.reducers[name]
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



val opSerializer = object : Serializer<Op<SKVI>,Op<SKVI>> {
  override fun serializeToString(obj: Op<SortedKeyValueIterator<Key, Value>>): String {
    return SerializationUtil.serializeBase64(obj)
  }

  @Suppress("UNCHECKED_CAST")
  override fun deserializeFromString(str: String): Op<SortedKeyValueIterator<Key, Value>> {
    return SerializationUtil.deserializeBase64(str) as Op<SortedKeyValueIterator<Key, Value>>
  }
}

fun skviOpToTask(
    op: Op<SKVI>,
    accumuloConfig: AccumuloConfig,
    scanTable: String
): Callable<LinkedHashMap<Key, Value>> {
  return OpAccumuloPipelineTask(OpAccumuloPipeline(op, opSerializer, scanTable), accumuloConfig)
}




data class AccumuloPlan(
    val op: Op<*>,
    val sap: SortedAccessPath,
    val scanTable: String?,
    val tasksBefore: List<Callable<*>>,
    val tasksAfter: List<Callable<*>>
)


fun racoToAccumulo(
    /** top-down recursion */
    ro: RacoOperator,
    /** Config info for the Accumulo that a Store writes to. */
    accumuloConfig: AccumuloConfig,
    /** A requirement on the access path. Generated by joins and group bys.
     * If the requirement cannot be met, then we must re-sort and do another pass.  */
    req: APReq = APReq.NoReq
): AccumuloPlan {
  @Suppress("UNCHECKED_CAST")
  return when (ro) {

    is Store -> {
      val tableName = ro.relationKey.sanitizeTableName()

      val pp = racoToAccumulo(ro.input, accumuloConfig, req)
      pp.op as Op<TupleIterator>

      val kvi = OpTupleToKeyValueIterator(pp.op, pp.sap, pp.sap)
      val skvi = OpKeyValueToSkviAdapter(kvi)
      val rwi = OpRWI(skvi, tableName, accumuloConfig)

      val newtasks = pp.tasksBefore + CreateTableTask(tableName, accumuloConfig)

      AccumuloPlan(rwi, pp.sap, pp.scanTable, newtasks, pp.tasksAfter)
    }

    is Apply -> {
      val pp = racoToAccumulo(ro.input, accumuloConfig, req)
      pp.op as Op<TupleIterator>

      var emittersRaco: List<Pair<Name, RacoExpression>> = ro.emitters
      // take out family expression if it is there
      val famExpr: Expr<ABS> = emittersRaco.find { it.first == __FAMILY__ }?.let {
        emittersRaco -= it
        racoExprToExpr(it.second, pp.sap)
      } ?: TupleRef.RefFamily()

      // todo - build a map of attribute names to visibility expressions, and another one for timestamp expressions
      val map_VIS: Map<Name, Expr<ABS>> = emittersRaco.partition { it.first.endsWith(__VIS) }.let {
        emittersRaco = it.second
        it.first.map {
          it.first.substring(0, it.first.length - __VIS.length) to
              racoExprToExpr(it.second, pp.sap)
        }.toMap()
      }

      val map_TS: Map<Name, Expr<Long>> = emittersRaco.partition { it.first.endsWith(__TS) }.let {
        emittersRaco = it.second
        it.first.map {
          val type = it.second.getType(pp.sap)
          it.first.substring(0, it.first.length - __TS.length) to
              type.decodeToLong(racoExprToExpr(it.second, pp.sap))
        }.toMap()
      }


      val exprInfos: List<Triple<Name, Expr<ArrayByteSequence>, Type<*>>> = emittersRaco.map {
        Triple(it.first, racoExprToExpr(it.second, pp.sap), it.second.getType(pp.sap)) }

      val emittersType: List<Pair<Name, Type<*>>> = emittersRaco.map { it.first to it.second.getType(pp.sap) }

      var emittersScheme: AccessPath
      val keyExprs: List<Expr<ABS>>
      val valExprs: List< Pair<ABS,Expr<FullValue>> >
      // if __DAP__ and/or __LAP__ were present as emitted attributes, then they are accounted for in emittersScheme
      // if both were not present, then all attributes are in the DAP
      var (dap, lap, cap) = if (emittersType.any() { it.first == __DAP__ }) {
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
            pp.sap.dapNames.contains(name) -> dap
            pp.sap.lapNames.contains(name) -> lap
            pp.sap.valNames.contains(name) -> cap
            expr.inputs.isEmpty() -> cap
            expr.inputs.all { it is TupleRef.RefKey && it.keyNum < pp.sap.dapNames.size } -> dap
            expr.inputs.all { it is TupleRef.RefKey } -> lap
            else -> cap
          }.add(exprInfo)
          partitions
        }
        val (dap,lap,cap) = trip

        emittersScheme = AccessPath.of(dap.map { it.first }, lap.map { it.first }, cap.map { it.first }, (dap+lap+cap).map { it.third })
        trip
      }

      // if dap0 is empty and lap0 has an element, then upgrade the first element in lap to dap
      if (dap.isEmpty() && lap.isNotEmpty()) {
        val first = lap.first()
        dap = listOf(first)
        lap -= first
        val firstName = emittersScheme.lapNames.first()
        val dapNames = listOf(firstName)
        val lapNames = emittersScheme.lapNames - firstName
        emittersScheme = AccessPath.of(dapNames, lapNames, emittersScheme.valNames, emittersScheme.types, emittersScheme.widths)
      }

      keyExprs = (dap+lap).map { it.second }
      valExprs = cap.map { it ->
        val name = it.first

        // If we have an expression for the visibility or timestamp, use it!
        // If parent had the same value attribute defined, then pull the visibility and ts from it.
        // Otherwise use the Empty visibility and Long.MAX_VALUE timestamp
        val expr_VIS = when (name) {
          in map_VIS -> map_VIS[name]!!
          in pp.sap.valNames -> nameToValueFirstVisRef(name)
          else -> Const(EMPTY)
        }
        val expr_TS = when (name) {
          in map_TS -> map_TS[name]!!
          in pp.sap.valNames -> nameToValueFirstTsRef(name)
          else -> Const(Long.MAX_VALUE)
        }
        name.toABS() to exprToFullValueExpr(it.second, expr_VIS, expr_TS)
      }

      // finally, calculate the sortedUpto
      // todo - find the longest prefix of keys that were copied from the parent. Take the minimum of that length and the parent's sortedUpto.
      val sortedUpto = 0
      val sap = emittersScheme.withSortedUpto(sortedUpto)

      AccumuloPlan(OpApplyIterator(pp.op, keyExprs = keyExprs, famExpr = famExpr, valExprs = valExprs), sap,
          pp.scanTable, pp.tasksBefore, pp.tasksAfter)
    }

    is Scan -> {
      val tableName = ro.relationKey.sanitizeTableName()

      val types: List<Pair<Name, Type<*>>> = getTypesFromScheme(ro.scheme)
      val ap: AccessPath = fromRacoScheme(types)
      // fully sorted, since we are scanning from a table
      val sortedUpto = ap.dapNames.size+ap.lapNames.size
      val sap = ap.withSortedUpto(sortedUpto)

      AccumuloPlan(OpAccumuloBase(sap, sap), sap, tableName, listOf(), listOf())
    }

    is FileScan -> {
      // get the encoders; ensure we store properly; might need to implement the getExpressionProperties on other operators
      val types: List<Pair<Name, Type<*>>> = getTypesFromScheme(ro.scheme)
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

      AccumuloPlan(OpCSVScan(ro.file, encoders, types, skip = skip), sap, null, listOf(), listOf())
    }

    else -> throw UnsupportedOperationException("unsupported in the raco-to-accumulo compiler: $ro")
  }

}


private fun getTypesFromScheme(scheme: List<Pair<Name, RacoType>>): List<Pair<Name, Type<*>>> {
  val ns: List<Pair<Name, RacoType>> = scheme
  return ns.map { it.first to racoTypeToType(it.second) }
}



sealed class APReq {

  object NoReq : APReq()

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
fun RelationKey.sanitizeTableName(): String = "${this.user}_${this.program}_${this.relation}"


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



