package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import org.apache.accumulo.core.data.ArrayByteSequence


sealed class TupleRef<out R>(args: List<Op<*>> = emptyList()) : Expr<R>(args) {
  constructor(vararg args: Op<*>): this(ImmutableList.copyOf(args))
  abstract val tupleNum: Int
  override val inputs: Set<TupleRef<R>>
    get() = setOf(this)

  data class RefKey(
      override val tupleNum: Int = 0,
      val keyNum: Int
  ) : TupleRef<ArrayByteSequence>(Obj(tupleNum), keyNum.toObj()) {
    override fun eval(tuples: List<Tuple>): ArrayByteSequence = tuples[tupleNum].keys[keyNum]
  }

  data class RefFamily(
      override val tupleNum: Int = 0
  ) : TupleRef<ArrayByteSequence>(Obj(tupleNum)) {
    override fun eval(tuples: List<Tuple>): ArrayByteSequence = tuples[tupleNum].family
  }

  data class RefVal(
      override val tupleNum: Int = 0,
      val valName: ArrayByteSequence
  ) : TupleRef<List<FullValue>>(Obj(tupleNum), Obj(valName)) {
    override fun eval(tuples: List<Tuple>): List<FullValue> = tuples[tupleNum].vals[valName]
  }

}

sealed class Expr<out R>(args: List<Op<*>> = emptyList()) : Op<R>(args) {
  constructor(vararg args: Op<*>): this(ImmutableList.copyOf(args))
  abstract val inputs: Set<TupleRef<*>>
  abstract fun eval(tuples: List<Tuple>): R

  override val unbound: List<Arg<*>> = listOf(Arg("tuples", List::class.java, null))

  override fun invoke(reqs: List<*>): R {
    // I don't think this does dynamic type checking. Let's assume we get good input.
    @Suppress("UNCHECKED_CAST")
    val tuples = reqs as List<Tuple>
    return eval(tuples)
  }
}

/** Convert a list of [R]s to a single [R].
 * Emits the default value if no values are present.
 * Uses the reducer to combine values together if at least one value is present
 */
data class DefaultExpr<R>(
    val expr: Expr<List<R>>,
    val default: R? = null,
    val reducer: (R, R) -> R = { a, b -> throw RuntimeException("did not expect > 1 values. The first two are $a and $b") }
) : Expr<R>(expr, default.toObj(), reducer.toObj()) {
  override val inputs: Set<TupleRef<*>> = expr.inputs
  override fun eval(tuples: List<Tuple>): R =
      expr.eval(tuples).let { if (it.isEmpty()) {default ?: throw RuntimeException("no default value provided but the value is missing")} else it.reduce(reducer) }
}

data class UnaryExpr<S, out R>(
    val expr: Expr<S>,
    val f: (S) -> R
) : Expr<R>(expr, f.toObj()) {
  override val inputs: Set<TupleRef<*>> = expr.inputs
  override fun eval(tuples: List<Tuple>): R {
    return f(expr.invoke(tuples))
  }
}

data class BinaryExpr<S1, S2, out R>(
    val e1: Expr<S1>,
    val e2: Expr<S2>,
    val f: (S1, S2) -> R
) : Expr<R>(e1, e2, f.toObj()) {
  override val inputs: Set<TupleRef<*>> = e1.inputs + e2.inputs
  override fun eval(tuples: List<Tuple>): R {
    return f(e1.invoke(tuples), e2.invoke(tuples))
  }
}

data class Const<out R>(
    val obj: R
) : Expr<R>(obj.toObj()) {
  override val inputs: Set<TupleRef<*>> = setOf()
  override fun eval(tuples: List<Tuple>): R = obj
}