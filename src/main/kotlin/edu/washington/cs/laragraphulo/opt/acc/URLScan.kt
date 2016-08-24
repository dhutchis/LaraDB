package edu.washington.cs.laragraphulo.opt.acc

import edu.washington.cs.laragraphulo.old.AccessPath
import edu.washington.cs.laragraphulo.opt.Obj
import edu.washington.cs.laragraphulo.opt.Op
import edu.washington.cs.laragraphulo.opt.IteratorFlow
import java.net.URL

class URLScan(val url: Obj<URL>,
              val schema: Obj<AccessPath>
) : Op<IteratorFlow>(url, schema) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}


