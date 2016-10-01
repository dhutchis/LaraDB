package edu.washington.cs.laragraphulo.opt.old

import com.google.common.collect.Iterators
import edu.washington.cs.laragraphulo.opt.Tuple


typealias ExtFun = (Tuple) -> Iterator<Tuple>

/**
 * Assumes that the iterator is in the order specified by a correct ImmutableAccessPath
 */
@Suppress("UNCHECKED_CAST")
fun Iterator<Tuple>.ext(f: ExtFun): Iterator<Tuple> {
  return Iterators.concat(Iterators.transform(this, f as (Tuple?) -> Iterator<Tuple>))
}



