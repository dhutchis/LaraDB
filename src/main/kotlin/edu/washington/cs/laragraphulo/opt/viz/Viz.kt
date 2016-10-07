package edu.washington.cs.laragraphulo.opt.viz

import edu.washington.cs.laragraphulo.opt.Op
import java.util.*


/**
 * Collect the nodes and edges of this graph.
 */
private fun Op<*>.collectEdges(edges: MutableMap<Op<*>,Set<Op<*>>> = HashMap()): Map<Op<*>,Set<Op<*>>> {
  val set = HashSet<Op<*>>()
  args.forEach { set.add(it) }
  edges.put(this, set)
  args.forEach { it.collectEdges(edges) }
  return edges
}

fun Op<*>.generateDot(title: String = ""): String {

  val edgeMap: Map<Op<*>, Set<Op<*>>> = collectEdges()
  val nodeStr = edgeMap
      .flatMap { it.value.map { it } + it.key }
      .map { """"${it.hashCode()}" [label="${it.toShortStr().replace("\"","\\\"")}"] ;""" }
      .joinToString(separator = "\n      ")
  val edgeStr = edgeMap
      .flatMap { pit -> pit.value.map { cit -> Pair(pit.key, cit) } }
      .map { """"${it.first.hashCode()}" -> "${it.second.hashCode()}" ;""" }
      .joinToString(separator = "\n      ")

  return """digraph G {
      ratio = "fill" ;
      size = "4.0, 4.0" ;
      page = "4, 4" ;
      margin = 0 ;
      mincross = 2.0 ;
      rankdir = "BT" ;
      nodesep = 0.25 ;
      ranksep = 0.25 ;
      node [fontname="Helvetica", fontsize=10,
            shape=oval, style=filled, fillcolor=white ] ;

      // The nodes
      $nodeStr

      // The edges
      $edgeStr

      // The title
      labelloc="t";
      label="${title.replace("\"","\\\"")}";
}"""
}