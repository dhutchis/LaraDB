package edu.washington.cs.laragraphulo.opt.raco

import edu.washington.cs.laragraphulo.opt.Op

class Select(val rel: Op<Relation>,
             val expr: Op<Expression<Boolean>>
) : Op<Relation>(rel, expr) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}


