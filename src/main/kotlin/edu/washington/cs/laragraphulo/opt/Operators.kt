package edu.washington.cs.laragraphulo.opt

import java.net.URL

class CSVScan(val url: Op<URL>,
              val accessPath: Op<AccessPath> // change this to CSVSchema - list of attributes
) : Op<IteratorFlow>(url, accessPath) {
  init {
    // todo: initial properties
  }
  // todo: add invoke execution
}

// class ChangeAccessPath -- destroys sort

// class RemoteStore - RemoteWriteIterator

// class

// class Sink - drop all entries - hasTop() always false