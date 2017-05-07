package edu.washington.cs.laragraphulo.api

import edu.washington.cs.laragraphulo.api.NameTupleOp.*
import edu.washington.cs.laragraphulo.api.NameTupleOp.MergeUnion0.*
import edu.washington.cs.laragraphulo.opt.SKVI


fun NameTupleOp.getBaseTables(): Set<Table> = when(this) {
  is Load -> setOf(this.table)
  is Ext -> this.parent.getBaseTables()
  is Empty -> setOf()
  is MergeUnion0 -> this.p1.getBaseTables() + this.p2.getBaseTables()
  is Rename -> this.p.getBaseTables()
  is Sort -> this.p.getBaseTables()
  is MergeJoin -> this.p1.getBaseTables() + this.p2.getBaseTables()
  is ScanFromData -> setOf()
}

fun NameTupleOp.lower(tableMap: Map<Table, SKVI>): NameTupleOp = when(this) {
  is Ext -> Ext(this.parent.lower(tableMap), this.extFun)
  is Empty -> this
  is MergeUnion -> MergeUnion(this.p1.lower(tableMap), this.p2.lower(tableMap), this.plusFuns)
  is MergeAgg -> MergeAgg(this.p1.lower(tableMap), keysKept, plusFuns)
  is Rename -> Rename(this.p.lower(tableMap), renameMap)
  is Sort -> Sort(p.lower(tableMap), newSort)
  is MergeJoin -> MergeJoin(this.p1.lower(tableMap), this.p2.lower(tableMap), timesFuns)
  is ScanFromData -> this
  is Load -> {
    require(this.table in tableMap) {"Attempt to lower a NameTupleOp stack but no SKVI given for $table"}
    // wrap around SKVI to convert Key/Value entries to a map. Need a Schema
    TODO()
  }
}

fun NameTupleOp.getBaseTables0(): Set<Table> {
  return this.fold(setOf<Table>(), {a,b -> a + b}) { when(it) {
    is Load -> setOf(it.table)
    else -> setOf()
  } }
}

//val f: (NameTupleOp) -> NameTupleOp = {
//  when (it) {
//    is Load -> {
//      it
//    }
//    else -> it
//  }
//}

//val x = X.transform { it }

// Ext( Load() )
//
