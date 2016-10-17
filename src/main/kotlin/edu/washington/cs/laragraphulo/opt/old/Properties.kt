package old

import com.google.common.collect.ImmutableSortedMap
import edu.washington.cs.laragraphulo.opt.old.Contradiction

/**
 * Union two maps together.
 * If both maps have a key, then they must have the same value for that key.
 * @return [m1] if the maps are equal (agree on everything), otherwise the newly merged map
 * @throws Contradiction If the maps both have a key but disagree on the value.
 */
private fun <K : Comparable<K>,V> mergeMaps(m1: ImmutableSortedMap<K, V>, m2: ImmutableSortedMap<K, V>): ImmutableSortedMap<K, V> {
  if (m1 == m2)
    return m1
  val builder = ImmutableSortedMap.naturalOrder<K,V>()
      .putAll(m1)
  m2.forEach {
    if (it.key in m1) {
      if (it.value != m1[it.key])
        throw Contradiction("Disagreement on key ${it.key}: ${m1[it.key]} vs ${it.value}")
    } else {
      builder.put(it)
    }
  }
  return builder.build()
}
/*

/**
 * Non-exclusive set of attributes in an [AssociativeTable].
 * The AssociativeTable may have more attributes present than the ones here.
 * The attributes are indexed by name in [allAttributes].
 */
open class Attributes protected constructor(
    val allAttributes: ImmutableSortedMap<String, Attribute<*>>
) : Property<AssociativeTable> {
  constructor(allNames: Collection<Attribute<*>>) : this(ImmutableSortedMap.copyOf(allNames.map { it.name to it }.toMap()))

  /**
   * If we conclude two [Attributes] that agree on everything, then no new results.
   * If we conclude two [Attributes] that are compatible
   * (they agree on all attributes in common and may have attributes not in common), then we take their union.
   * @throws Contradiction If we conclude incompatible [Attributes].
   */
  override fun merge(newp: Property<AssociativeTable>): Property.NewPropWithImplied<AssociativeTable, Property<AssociativeTable>> {
    if (newp !is Attributes) throw IllegalArgumentException("Merging a different kind of property! $newp")
    val newAttrs = Attributes(mergeMaps(allAttributes, newp.allAttributes))
    return Property.NewPropWithImplied(newAttrs)
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Attributes

    if (allAttributes != other.allAttributes) return false

    return true
  }
  override fun hashCode(): Int = allAttributes.hashCode()

  override fun toString(): String{
    return "Attributes($allAttributes)"
  }
}

open class Schema protected constructor(
    val keyAttributes: ImmutableSortedMap<String, Attribute<*>>,
    val valAttribtues: ImmutableSortedMap<String, Attribute<*>>
) : Attributes(
    ImmutableSortedMap.naturalOrder<String, Attribute<*>>()
        .putAll(keyAttributes)
        .putAll(valAttribtues)
        .build()
) {
  constructor(keyAttributes: Collection<Attribute<*>>,
              valAttributes: Collection<Attribute<*>>)
  : this(ImmutableSortedMap.copyOf(keyAttributes.map { it.name to it }.toMap()),
      ImmutableSortedMap.copyOf(valAttributes.map { it.name to it }.toMap()))

  override fun merge(newp: Property<AssociativeTable>): Property.NewPropWithImplied<AssociativeTable, Property<AssociativeTable>> {
    if (newp !is Schema) throw IllegalArgumentException("Merging a different kind of property! $newp")
    val newKeyAttrs = mergeMaps(keyAttributes, newp.keyAttributes)
    val newValAttrs = mergeMaps(valAttribtues, newp.valAttribtues)
    if (newKeyAttrs === keyAttributes && newValAttrs === newValAttrs)
      return Property.NewPropWithImplied(this)
    else
      return Property.NewPropWithImplied(Schema(newKeyAttrs, newValAttrs))
  }

  override fun toString(): String{
    return "Schema(keyAttributes=$keyAttributes, valAttribtues=$valAttribtues)"
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as Schema

    if (keyAttributes != other.keyAttributes) return false
    if (valAttribtues != other.valAttribtues) return false

    return true
  }

  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + keyAttributes.hashCode()
    result = 31 * result + valAttribtues.hashCode()
    return result
  }


}


open class AccessPath(
    /** distributed access path */
    val dapNames: ImmutableList<Attribute<*>>,
    /** local access path */
    val lapNames: ImmutableList<Attribute<*>>,
    /**
     * column access path
     * A list of the attribute groups. Each group is potentially stored in a different file.
     * The ordering of attributes within groups is lexicographic.
     */
    val cap: ImmutableSortedMap<String, ColumnFamily>
) : Schema(
    ImmutableSortedMap.naturalOrder<String, Attribute<*>>()
        .putAll(dapNames.map { it.name to it }.toMap())
        .putAll(lapNames.map { it.name to it }.toMap())
        .build(),
    ImmutableSortedMap.naturalOrder<String, Attribute<*>>()
        .putAll(cap.flatMap { it.value.attributes.entries }.map { it.key to it.value }.toMap())
        .build()
) {
  override fun merge(newp: Property<AssociativeTable>): Property.NewPropWithImplied<AssociativeTable, Property<AssociativeTable>> {
    if (newp !is AccessPath) throw IllegalArgumentException("Merging a different kind of property! $newp")
    if (dapNames != newp.dapNames)
      throw Contradiction("Different distributed access paths $dapNames vs ${newp.dapNames}")
    if (lapNames != newp.lapNames)
      throw Contradiction("Different distributed access paths $lapNames vs ${newp.lapNames}")
    if (cap == newp.cap)
      return Property.NewPropWithImplied(this)

    val newcapBuilder = ImmutableSortedMap.naturalOrder<String, ColumnFamily>()
    // both and just cap
    cap.forEach { cape ->
      val name = cape.key
      val cf: ColumnFamily
      if (name in newp.cap) {
        val cfmap = mergeMaps(cape.value.attributes, newp.cap[name]!!.attributes)
        cf = ColumnFamily(name, cfmap)
      } else {
        cf = cape.value
      }
      newcapBuilder.put(name, cf)
    }
    // just newp.cap
    newp.cap.forEach { cape ->
      val name = cape.key
      if (name !in cap)
        newcapBuilder.put(name, cape.value)
    }
    val ncap = newcapBuilder.build()
//    if (ndap === dapNames && nlap === lapNames && ncap === cap)
//      return Property.NewPropWithImplied(this)
//    else
    return Property.NewPropWithImplied(AccessPath(dapNames, lapNames, ncap))
  }

  override fun toString(): String{
    return "AccessPath(dapNames=$dapNames, lapNames=$lapNames, cap=$cap)"
  }

  override fun equals(other: Any?): Boolean{
    if (this === other) return true
    if (other?.javaClass != javaClass) return false

    other as AccessPath

    if (dapNames != other.dapNames) return false
    if (lapNames != other.lapNames) return false
    if (cap != other.cap) return false

    return true
  }

  override fun hashCode(): Int{
    var result = super.hashCode()
    result = 31 * result + dapNames.hashCode()
    result = 31 * result + lapNames.hashCode()
    result = 31 * result + cap.hashCode()
    return result
  }



  // an access path fragment is an access path that may have a subset of the column access paths

//  open class AccumuloFlow(
//
//  ) : AccessPath

  // context: Scan, Minc, Majc


}*/
