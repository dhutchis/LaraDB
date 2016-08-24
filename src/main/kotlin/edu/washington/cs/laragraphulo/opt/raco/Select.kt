package edu.washington.cs.laragraphulo.opt.raco

import edu.washington.cs.laragraphulo.opt.AssociativeTable
import edu.washington.cs.laragraphulo.opt.Expression
import edu.washington.cs.laragraphulo.opt.Op
import edu.washington.cs.laragraphulo.opt.Relation

class Select(val rel: Op<AssociativeTable>,
             val expr: Op<Expression<Boolean>>
) : Op<AssociativeTable>(rel, expr) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}


