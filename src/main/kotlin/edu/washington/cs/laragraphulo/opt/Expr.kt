package edu.washington.cs.laragraphulo.opt

import org.apache.accumulo.core.data.ArrayByteSequence


sealed class TupleRef<out R>(
    open val tupleNum: Int = 0
) : Expr<R> {
  override val inputs: Set<TupleRef<R>>
    get() = setOf(this)

  data class RefKey(
      override val tupleNum: Int = 0,
      val keyNum: Int
  ) : TupleRef<ArrayByteSequence>(tupleNum) {
    override operator fun invoke(tuples: List<Tuple>): ArrayByteSequence = tuples[tupleNum].keys[keyNum]
  }

  data class RefFamily(
      override val tupleNum: Int = 0
  ) : TupleRef<ArrayByteSequence>(tupleNum) {
    override operator fun invoke(tuples: List<Tuple>): ArrayByteSequence = tuples[tupleNum].family
  }

  data class RefVal(
      override val tupleNum: Int = 0,
      val valName: ArrayByteSequence
  ) : TupleRef<List<FullValue>>(tupleNum) {
    override operator fun invoke(tuples: List<Tuple>): List<FullValue> = tuples[tupleNum].vals[valName]
  }

}

interface Expr<out R> {
  val inputs: Set<TupleRef<*>>
  operator fun invoke(tuples: List<Tuple>): R
}

/** Convert a list of [R]s to a single [R].
 * Emits the default value if no values are present.
 * Uses the reducer to combine values together if at least one value is present
 */
class DefaultExpr<R>(
    val expr: Expr<List<R>>,
    val default: R,
    val reducer: (R, R) -> R = { a, b -> throw RuntimeException("did not expect > 1 values. The first two are $a and $b") }
) : Expr<R> {
  override val inputs: Set<TupleRef<*>> = expr.inputs
  override fun invoke(tuples: List<Tuple>): R = expr.invoke(tuples).let { if (it.isEmpty()) default else it.reduce(reducer) }
}

class CastExpr<S,out R>(
    val expr: Expr<S>,
    val convert: (S) -> R
) : Expr<R> {
  override val inputs: Set<TupleRef<*>> = expr.inputs
  override fun invoke(tuples: List<Tuple>): R {
    return convert(expr.invoke(tuples))
  }
}

class BinaryExpr<S1, S2, out R>(
    val e1: Expr<S1>,
    val e2: Expr<S2>,
    val f: (S1, S2) -> R
) : Expr<R> {
  override val inputs: Set<TupleRef<*>> = e1.inputs + e2.inputs
  override fun invoke(tuples: List<Tuple>): R {
    return f(e1.invoke(tuples), e2.invoke(tuples))
  }
}
