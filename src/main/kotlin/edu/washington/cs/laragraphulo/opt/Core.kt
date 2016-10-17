package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import java.io.Serializable
import java.lang.reflect.Constructor
import kotlin.reflect.KFunction


data class Arg<T>(
    val name: String,
    val type: Class<T>,
    /** If null, then this is a required argument. */
    val default: T? = null
) : Serializable

class InvokeException(val op: Op<*>,
                      msg: String = "problem invoking $op",
                      cause: Throwable? = null): RuntimeException(msg, cause)

/**
 * Similar to [KFunction]. Consider subclassing in the future.
 * The [Function] interface is semi-useless ~.~
 */
abstract class Op<out R>(args: List<Op<*>> = emptyList()) : Serializable, Function1<List<*>,R> {
  constructor(vararg args: Op<*>): this(ImmutableList.copyOf(args))
  val args: List<Op<*>> = ImmutableList.copyOf(args)

  /**
   * Additional parameters that must be provided to [invoke] in order to obtain an [R].
   * Must include children's [unbound]; this is a deep field.
   */
  abstract val unbound: List<Arg<*>>

  /** Call by name. Cannot do variable-argument construction, unless one of the arguments is a list.
   * Recommended to call [checkReqs]. */
  fun invokeBy(argMap: Map<String,*>): R {
    return invoke(unbound.map { argMap[it.name] })
  }

  /**
   * Check whether the arguments given in reqs match the required types.
   * Null may be present in reqs. If so, the argument it goes to must have a default.
   * The order of reqs must match that of [unbound].
   */
  fun checkReqs(reqs: List<*>): List<*> {
    if (reqs.size != unbound.size) throw InvokeException(this, "incorrect number of arguments given; expected $unbound; got $reqs")
    return reqs.zip(unbound).map {
      it.first?.apply {
        if (!it.second.type.isInstance(this)) throw InvokeException(this@Op, "bad argument type for ${it.second}: $this")
      } ?: it.second.default ?: throw InvokeException(this, "no argument given for ${it.second}")
    }
  }

  /** Call by position. Recommended to use [checkReqs] to check the arguments.
   * Order must match that of [unbound]. */
  override abstract operator fun invoke(reqs: List<*>): R

  open fun toShortStr(): String = javaClass.simpleName
  override fun toString(): String = "${javaClass.simpleName}($args)"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Op<*>

    if (args != other.args) return false

    return true
  }
  override fun hashCode(): Int = args.hashCode()
}

class Obj<out R>(val obj: R): Op<R>() {
  override val unbound: List<Arg<*>> = ImmutableList.of()
  override fun invoke(reqs: List<*>): R = obj
  override fun toString() = obj.toString()
  override fun toShortStr(): String = (obj as Any?)?.javaClass?.simpleName ?: "null"

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Obj<*>

    if (obj != other.obj) return false

    return true
  }
  override fun hashCode(): Int = (obj?.hashCode() ?: 0)
}

fun <R> R.toObj()= Obj(this)

//fun <R> R.toOp(): Op<R> = object : Op<R>() {
//  override fun invoke(reqs: List<*>): R = this@toOp
//  override fun toString() = this@toOp.toString()
//}

//val b: KFunction<Obj<*>> = Obj::class.primaryConstructor!!
//val c: Obj<*> = b.call(5)

//val d: Int = 55.toOp()()


fun <R> Constructor<R>.toOp(): Op<R> = ConstructorOp(this)

class ConstructorOp<R>(val constructor: Constructor<R>): Op<R>(Obj(constructor)) {
  override val unbound: List<Arg<*>> = constructor.parameters.map {
      @Suppress("UNCHECKED_CAST")
      (edu.washington.cs.laragraphulo.opt.Arg<Any?>(it.name, it.type as Class<Any?>, null))
    }

  override fun invoke(reqs: List<*>): R {
    return constructor.newInstance(*reqs.toTypedArray())
  }

  override fun toString(): String = "${constructor.declaringClass.simpleName}()"
}

