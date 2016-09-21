package edu.washington.cs.laragraphulo.opt

import com.google.common.collect.ImmutableList
import java.util.*

/** Thrown if Drools concludes contradictory facts */
class Contradiction(vararg objs: Any) : RuntimeException(objs.joinToString(separator = "\n\t"))



//interface SelfMerging<T : SelfMerging<T, R>, R> {
//    fun merge(newp: T): NewPropWithImplied<R, T>
//}


/**
 * Properties should be immutable!
 */
interface Property<R> {

  /**
   * Returned by [Property.merge].
   * The [newprop] is the updated property, and
   * [implied] is the set of newly implied properties resulting from it.
   */
  data class NewPropWithImplied<in R, out P: Property<in R>>(val newprop: P,
                                                             val implied: Set<Property<in R>> = emptySet())

  /**
   * Return properties resulting from merging this property with another.
   * Throw a [Contradiction] if incompatible.
   * If this property needs updating as a result of the merge,
   * return the updated property, along with all implied properties that were not implied before.
   * Return `Pair(this, emptySet())` if no update is necessary.
   * The `this` indicates that the property has not changed by referential equality.
   *
   * Example: let property P1 be HasAttributeNameAndType({(Age -> int)})
   * and P2 be HasAttributeNameAndType({(Name -> String)}).
   * P1.impliedProperties() returns {HasAttributeName({Age})}.
   * P2.impliedProperties() returns {HasAttributeName({Name})}.
   * P1.merge(P2) returns Pair(HasAttributeNameAndType({(Age -> int), (Name -> String)}), {HasAttributeName({Age, Name})}).
   * P1.merge(P1) returns Pair(this, {}).
   */
  fun merge(newp: Property<R>): NewPropWithImplied<R, Property<R>> {
    if (newp.javaClass != javaClass) throw IllegalArgumentException("Merging a different kind of property! $newp")
    if (this != newp) throw Contradiction("Concluded differing properties, and no resolution has been defined!", this, newp)
    return NewPropWithImplied(this)
  }

  /**
   * Properties implied by the existence of this one.
   * In terms of the lattice of properties, these are the properties *less than* this one.
   *
   * For example, Sorted(["id", "age"]) implies the properties HasAttributeName({"id", "age"})
   */
  fun impliedProperties(): Set<Property<in R>> = emptySet()
  // for each p in impliedProperties()
  // assert(p.javaClass in Property.ImpliedEquivalenceMap[this.javaClass], "broken equivalence implication contract")

  /**
   * Equivalences implied by an equivalence of this class.
   * This companion object implements these methods "statically".
   *
   * Contract: if it is possible for an instance of this class to imply a property,
   * then this class should imply an equivalence of that property's class when there is an
   * equivalence for this class.
   */
  companion object /*ImpliedEquivMap*/ {
    private val map: MutableMap<Class<out Property<*>>, MutableSet<Class<out Property<*>>>> = hashMapOf()

    @Suppress("UNCHECKED_CAST")
    fun <L>getImpliedEquiv(c: Class<out Property<out L>>): Set<Class<out Property<in L>>> =
        map[c]?.toSet() as Set<Class<out Property<in L>>>?
            ?: emptySet<Class<out Property<in L>>>()

    fun <L>addImpliedEquiv(c: Class<out Property<out L>>,
                           vararg ps: Class<out Property<in L>>) {
      if (c !in map)
        map.put(c, hashSetOf(*ps))
      else
        map[c]!!.addAll(ps)
    }

    // idea: add a method to print the lattice of properties by equivalence
    // would be nice if properties registered themselves in a global class map
  }
}

/**
 * Tracks the properties and their equivalences we have concluded for an Op.
 * An Op has either zero or one instances of a property type.
 * Uses a union find-like method to track equivalences of properties.
 * The following cases outline the possible status for a property of type T that an object O can have:
 *
 * 1. No information about whether O has a property of type T.
 * 2. O has the same property of type T as some collection of object O2, O3, ..., On,
 * but what the property is is unknown. `isEquiv(O1,O2)==true; isEquiv(O1,O3)==true;` etc.
 * 3. O has the property P of type T.  `get(T) == P`
 * 4. O has the same property of type T as some collection of object O2, O3, ..., On,
 * and all these objects have the same property P. `get(T) == P; isEquiv(O1,O2)==true; isEquiv(O1,O3)==true;` etc.
 *
 * This class tracks which EPropMaps are in the same equivalence class for each property. See [getEquiv].
 */
open class EPropMap<L> {
//  private val testMap = com.google.common.collect.MutableClassToInstanceMap.create<Property<in L>>()
  private val nodeMap: MutableMap<Class<out Property<in L>>, Node<in L, out Property<in L>>> = hashMapOf()

  /**
   * All properties associated in the EPropMap, keyed by class.
   */
  val propMap: Map<Class<out Property<in L>>, Property<in L>>
    get() {
      return nodeMap.mapValues{
        e ->
        // I can't convince the type-checker to allow this statement, so I rewrote getNodeInner
//        this.getNodeInner(e.key, e.value).content!!
        val (c, n1) = e
        val n2 = e.value.find()
        if (n1 !== n2 && n1.backset.size == 1 && n1.backset.single() === this) {
          // path compression: connect directly to end node when safe to do so
          nodeMap.put(c, n2)
          n1.backset.clear() // n1 is now eligible for garbage collection
        }
        n2.content!!
      }
    } // computed on the fly


  private companion object {
    // map for this class is out Property<in TupleFunction> -- let L:R=TupleFunction:Function, stores
    // actual R=Function, P:Property<R>=Inputs:Property<Function>
    @Suppress("UNCHECKED_CAST")
    private fun <H, P: Property<H>, L: H>mapGet(map: Map<Class<out Property<in L>>, Node<in L, out Property<in L>>>,
                                                c: Class<P>): Node<H, P>? = map[c] as Node<H, P>?

    // map for this class is for L:R=TupleFunction:Function
    // want to put R=Function, P:Property<R>=Inputs:Property<Function>
    inline fun <H, reified P: Property<H>, L: H>mapPut(map: MutableMap<Class<out Property<in L>>, Node<H, P>>,
                                                       n: Node<H, P>) {
      map.put(P::class.java, n)
    }
  }

  private fun <H, P: Property<H>, L:H> EPropMap<L>
      .getNode(c: Class<P>): Node<H, P>? {
    val n1 = mapGet(nodeMap, c) ?: return null
    return getNodeInner(c, n1)
  }

  private fun <H, P: Property<H>, L:H> EPropMap<L>.getNodeInner(c: Class<P>, n1: Node<H, P>): Node<H, P> {
    val n2 = n1.find()
    if (n1 !== n2 && n1.backset.size == 1 && n1.backset.single() === this) {
      // path compression: connect directly to end node when safe to do so
      nodeMap.put(c, n2)
      n1.backset.clear() // n1 is now eligible for garbage collection
    }
    return n2
  }

//    private inline fun <H, reified P:Property<H>, L:H> EPropMap<L>
//            .putNode(n: Node<H,P>) {
//        nodeMap.put(P::class.java,n)
//    }

  private fun <H, P: Property<H>, L:H> EPropMap<L>
      .putNode(c: Class<P>, n: Node<H, P>) {
    nodeMap.put(c, n)
  }



  /** Get the Property for this property type in the map, if one exists. Null otherwise.
   * The shorthand for this function is `epropmap[c]`. */
  //    @Suppress("UNCHECKED_CAST")
  operator fun <H, P: Property<H>, L:H> EPropMap<L>
      .get(c: Class<P>): P? = getNode(c)?.content //as P?

  /** The shorthand for this function is `c in epropmap` or `c !in epropmap`.
   * Implemented as `epropmap[c] == null`? See [get]. */
  operator fun <H, L:H> EPropMap<L>
      .contains(c: Class<out Property<H>>): Boolean = get(c) != null

  /**
   * Return the set of EPropMaps that are equivalent for this property.
   */
  fun <H, P: Property<H>, L:H> EPropMap<L>
      .getEquiv(c: Class<P>): Set<EPropMap<out H>> {
    return getNode(c)?.backset ?: return setOf(this)
  }

  // consider adding an addAll method that takes an Iterable<Property<R>>
  // and making this one call addAll.
  // Make addAll tail-recursive by chaining iterators.
  /**
   * Add a property.
   * @return whether anything has changed as a result of adding the property. False if nothing changed.
   */
  fun <H, L:H> EPropMap<L>
      .add(p: Property<H>): Boolean {
    val c = p.javaClass
    val newimp: Set<Property<in L>>
    val changed: Boolean
    if (c !in nodeMap) {
      nodeMap.put(c, Node(p, this))
      newimp = p.impliedProperties()
      changed = true
    } else {
      val n = getNode(c)!! //nodeMap[c]!!.find()
      if (n.content == null) {
        n.content = p
        newimp = p.impliedProperties()
        changed = true
      } else {
        val (newprop, implied) = n.content!!.merge(p)
        if (newprop !== n.content) {
          n.content = newprop
          changed = true
        } else
          changed = implied.isNotEmpty()
        newimp = implied
      }
    }
    newimp.forEach { add(it) }
    return changed
  }

  /**
   * Equate two EPropMaps to be equivalent for the given property.
   * @return whether anything has changed as a result of adding the equivalence. False if nothing changed.
   */
  fun <H, L:H, U:H, P: Property<H>> EPropMap<L>
      .equiv(c: Class<P>, omap: EPropMap<U>): Boolean {
    // if already equivalent, do nothing
    if (isEquiv(c, omap)) return false

    // do the implied equivalences first
    Property.getImpliedEquiv(c).forEach {
      it: Class<out Property<in H>> ->
      @Suppress("UNCHECKED_CAST")
      equiv(it as Class<out Property<Any?>>, omap)
    }

    if (c !in nodeMap && c !in omap.nodeMap) {
      // neither this nor omap has an entry for c
      // add a new empty Node and connect this and omap to it
      val n: Node<H, Property<H>> = Node(this, omap)
      this.nodeMap.put(c, n)
      omap.nodeMap.put(c, n) // <H,Property<H>,U>
    } else if (c in nodeMap && c !in omap.nodeMap || c !in nodeMap && c in omap.nodeMap) {
      // mapin has an entry for c but mapout does not
      // connect mapout to the node in mapin and increment its rank
      val (mapin, mapout) = if (c in nodeMap) Pair(this, omap) else Pair(omap, this)
      val n = mapin.getNode(c)!! // I decided to eagerly traverse rather than wait until later
      mapout.putNode(c, n)
      n.backset.add(mapout)
    } else {
      // both this and omap have an entry for c
      // point the lower rank Node nlow to the higher rank Node nhigh; promote nhigh's rank
      // merge nlow's contents, if any, into nhigh's contents, if any
      // demote nlow to non-root-Node status (rank=0, contents=null)
      // if the merge creates new implied properties, add them in
      val n1 = getNode(c)!! //as Node<H,P> //nodeMap[c]!!.find()
      val n2 = omap.getNode(c)!! //nodeMap[c]!!.find()
      val (nlow, nhigh) = if (n1.backset.size < n2.backset.size) Pair(n1, n2) else Pair(n2, n1)
      nlow.ptr = nhigh// path compression: connect directly to end node, decrementing ranks of middle nodes
      nhigh.backset.addAll(nlow.backset)
      if (nlow.content == null) ;
      else if (nhigh.content == null) {
        nhigh.content = nlow.content
        nlow.content = null
      } else {
        val (newprop, implied) = nhigh.content!!.merge(nlow.content!!)
        nlow.content = null
        @Suppress("UNCHECKED_CAST") // if we were rigorous with F-Bounded Quantification, this would not be necessary
        if (newprop !== nhigh.content)
          nhigh.content = newprop as P
        implied.forEach { add(it) }
      }
    }
    return true
  }

  fun <H, P: Property<H>, L:H> EPropMap<L>
      .isEquiv(c: Class<P>, omap: EPropMap<out H>): Boolean =
      nodeMap[c]?.let { a -> omap.nodeMap[c]?.let { b -> a.find() === b.find() } } ?: false

  // The design of this class needs re-thinking if we want thread-safe access to EPropMap
  private class Node<L, P : Property<in L>>(vararg backers: EPropMap<out L>) {
    /** Union find pointer to the representative node. */
    var ptr: Node<L, P> = this
    /** Content of root nodes. Contract: `content != null` if and only if `ptr === this`. */
    var content: P? = null
    /** Number of pointers to this node. If `rank == 1`, then a map can safely discard this. */
    val backset: MutableSet<EPropMap<out L>> = hashSetOf(*backers) // I hope this does not need to be IdentityHashSet

    constructor(content: P, vararg backers: EPropMap<out L>) : this(*backers) {
      this.content = content
    }

    /** union find */
    fun find(): Node<L, P> {
      if (this === ptr)
        return this
      ptr = ptr.find0(backset)
      return ptr
    }

    /** path compression: connect directly to end node, decrementing ranks of middle nodes */
    private fun find0(backers: Set<EPropMap<out L>>): Node<L, P> {
      if (this === ptr)
        return this
      ptr = ptr.find0(backers + backset)
      backset.removeAll(backers)
      return ptr
    }
  }

  override fun toString(): String = "{" +
  nodeMap.keys.map { it: Class<out Property<in L>> ->
    @Suppress("UNCHECKED_CAST")
    get(it as Class<Property<Any?>>).toString()
  }.reduce { p1, p2 -> p1+", "+p2 } + "}"
}


/**
 *
 *
 * See [edu.washington.cs.laragraphulo.opt.viz.generateDot] to generate a visualization of the operator tree
 */
abstract class Op<R>(args: List<Op<*>> = emptyList()) : EPropMap<R>() {
  val args: List<Op<*>> = ImmutableList.copyOf(args)

  constructor(vararg args: Op<*>): this(args.asList())

  override fun toString() = this.javaClass.simpleName + args + "+Props:" + super.toString()

  /**
   * A short string describing this operator and not any of the children.
   * Used for visualizing the Op.
   */
  open fun toShortStr(): String = this.javaClass.simpleName //+ "+Props:" + super.toString()

  /**
   * Run this operator on a concrete object
   */
  open operator fun invoke(vararg argobs: Any): R {
    throw NotImplementedError()
  }

  /** Structural Equality / Referential Transparency:
   * two Ops are equal if they are the same class and have equal argument subtrees. */
  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Op<*>

    if (args != other.args) return false

    return true
  }
  override fun hashCode(): Int = args.hashCode()
}

/**
 * A constant, immutable Object. No children because any children that could have formed this object
 * can be collapsed into this single node.
 *
// * For objects that have properties, recommended to subclass this object and fill in the properties via [objProps].
 */
class Obj<R>(val obj: R): Op<R>() {
//    init {
//        objProps().forEach { add(it) }
//    }
//
//    /**
//     * Get the properties fully determined by obj.
//     * This should be all properties.
//     *
//     * It would be good to add a check that every property applicable to type R
//     * is determined by Obj.
//     */
//    fun objProps(): Set<Property<R>> = emptySet()

  override operator fun invoke(vararg argobs: Any): R {
    require(argobs.isEmpty()) {"No arguments expected to an Obj. Given $argobs"}
    return obj
  }

  override fun toString() = "Obj($obj)"
  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false
    other as Obj<*>
    if (obj != other.obj) return false
    return true
  }
  override fun hashCode(): Int = obj?.hashCode() ?: 0
}


/**
 * A constant that has certain specified properties.
 * This is different from an Obj in that the object is not fully known.
 *
 * This class violates Referential Transparency.
 * As such it overrides equals() and hashCode().
 */
class ConstantProps<R>(private val props: Set<Property<R>> = emptySet()) : Op<R>() {
  init {
    props.forEach { add(it) }
  }

  constructor(prop: Property<R>): this(setOf(prop))

  override fun equals(other: Any?): Boolean = this === other
  override fun hashCode(): Int{
    return props.hashCode()
  }
}





//// ********* Test
//fun main(args:Array<String>) {
//
//}

