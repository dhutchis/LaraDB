package edu.washington.cs.laragraphulo.opt.raco

import edu.washington.cs.laragraphulo.opt.AssociativeTable
import edu.washington.cs.laragraphulo.opt.ExpressionOld
import edu.washington.cs.laragraphulo.opt.Op

class Select(val rel: Op<AssociativeTable>,
             val expr: Op<ExpressionOld<Boolean>>
) : Op<AssociativeTable>(rel, expr) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}


