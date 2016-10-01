package edu.washington.cs.laragraphulo.opt

import org.apache.accumulo.core.data.ArrayByteSequence


sealed class TupleRef<R>(args: List<Op<*>> = emptyList()) : Expr<R>(args) {
  constructor(vararg args: Op<*>): this(args.asList())
  abstract val tupleNum: Obj<Int>
  override val inputs: Set<TupleRef<R>>
    get() = setOf(this)

  data class RefKey(
      override val tupleNum: Obj<Int> = Obj(0),
      val keyNum: Obj<Int>
  ) : TupleRef<ArrayByteSequence>(tupleNum, keyNum) {
    override operator fun invoke(tuples: List<Tuple>): ArrayByteSequence = tuples[tupleNum()].keys[keyNum()]
  }

  data class RefFamily(
      override val tupleNum: Obj<Int> = Obj(0)
  ) : TupleRef<ArrayByteSequence>(tupleNum) {
    override operator fun invoke(tuples: List<Tuple>): ArrayByteSequence = tuples[tupleNum()].family
  }

  data class RefVal(
      override val tupleNum: Obj<Int> = Obj(0),
      val valName: Obj<ArrayByteSequence>
  ) : TupleRef<List<FullValue>>(tupleNum, valName) {
    override operator fun invoke(tuples: List<Tuple>): List<FullValue> = tuples[tupleNum()].vals[valName()]
  }

}

sealed class Expr<R>(args: List<Op<*>> = emptyList()) : Op<R>(args) {
  constructor(vararg args: Op<*>): this(args.asList())
  abstract val inputs: Set<TupleRef<*>>
  abstract operator fun invoke(tuples: List<Tuple>): R
}

/** Convert a list of [R]s to a single [R].
 * Emits the default value if no values are present.
 * Uses the reducer to combine values together if at least one value is present
 */
class DefaultExpr<R>(
    val expr: Expr<List<R>>,
    val default: Obj<R?> = Obj(null),
    val reducer: Obj<(R, R) -> R> = Obj() { a, b -> throw RuntimeException("did not expect > 1 values. The first two are $a and $b") }
) : Expr<R>(expr, default, reducer) {
  override val inputs: Set<TupleRef<*>> = expr.inputs
  override operator fun invoke(tuples: List<Tuple>): R = expr(tuples).let { if (it.isEmpty()) {default() ?: throw RuntimeException("no default value provided but the value is missing")} else it.reduce(reducer()) }
}

class UnaryExpr<S, R>(
    val expr: Expr<S>,
    val f: Obj<(S) -> R>
) : Expr<R>(expr, f) {
  override val inputs: Set<TupleRef<*>> = expr.inputs
  override operator fun invoke(tuples: List<Tuple>): R {
    return f()(expr.invoke(tuples))
  }
}

class BinaryExpr<S1, S2, R>(
    val e1: Expr<S1>,
    val e2: Expr<S2>,
    val f: Obj<(S1, S2) -> R>
) : Expr<R>(e1, e2, f) {
  override val inputs: Set<TupleRef<*>> = e1.inputs + e2.inputs
  override operator fun invoke(tuples: List<Tuple>): R {
    return f()(e1.invoke(tuples), e2.invoke(tuples))
  }
}

class Const<R>(
    val obj: Obj<R>
) : Expr<R>(obj) {
  override val inputs: Set<TupleRef<*>> = setOf()
  override fun invoke(tuples: List<Tuple>): R = obj()
}
