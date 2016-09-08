package edu.washington.cs.laragraphulo.old

import com.google.common.base.Preconditions
import edu.washington.cs.laragraphulo.opt.Attribute
import java.io.Serializable

/**
 * For `F | G H | [A B] [C]`:
 *
 * ```
 * |row |colF|colQ     | val   |
 * |----|----|---------|-------|
 * | f1 | 1  | g1 h1 a | a1 |
 * | f1 | 1  | g1 h1 b | b1 |
 * | f1 | 1  | g1 h2 a | a2 |
 * | f1 | 1  | g1 h2 b | b2 |
 * | f1 | 2  | g1 h1 c | c1 |
 * | f1 | 2  | g1 h2 c | c2 |
 * | f1 |    | g1 h1 z | z1 |
 *
 * Decided against:
 * | f1 | 1  | g1 h1   | a1 b1 |
 * | f1 | 2  | g1 h1   | c1 d1 |
 * | f1 | 3  | g1 h1   | e1    |
 * | f1 |    | g1 h1 z | z1    |
 * ```
 *
 * 1. The column family is `1`.
 * The column qualifier is like
 * [G H] is in the column qualifier.
 *
 *
 */
//class ImmutableAccessPath(
//    val distrIndex: List<Attribute<*>>,
//    val localIndex: List<Attribute<*>>,
//    /**
//     * A list of the attribute groups. Each group is potentially stored in a different file.
//     * The ordering of attributes within groups is lexicographic.
//     */
//    val attrGroups: List<Set<Attribute<*>>>
//) : Schema(keyAttributes = distrIndex.toSet()+localIndex.toSet(),
//    valAttributes = attrGroups.flatten().toSet())
//{
//  init {
//    val ct = distrIndex.count() + localIndex.count() + attrGroups.sumBy { it.count() }
//    val distct = distrIndex.map { it.name }.union(localIndex.map { it.name }).union(
//        attrGroups.flatMap { it.map { it.name } }
//    ).count()
//    Preconditions.checkArgument(ct == distct, "Some attributes have the same name! ", distrIndex, localIndex, attrGroups)
//  }
//
//
//}

