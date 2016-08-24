package edu.washington.cs.laragraphulo.opt

import java.net.URL

class URLScan(val url: Op<URL>,
              val accessPath: Op<AccessPath>
) : Op<IteratorFlow>(url, accessPath) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}

