package edu.washington.cs.laragraphulo.opt

import edu.mit.ll.graphulo.skvi.RemoteWriteIterator
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment

import java.io.IOException

/**
 * Holds the logic for a commutative and associative *Reduce* operation.
 *
 *
 * A class that receives every entry passed through a [RemoteWriteIterator].
 * It holds a byte[] sent to the client after all entries in a tablet are processed.
 * Initialized at the beginning of running. Updated from Key/Value pairs.
 * Must be capable of being sent in partial form to the client, if monitoring is enabled.
 *
 *
 * Lifecycle: init() will be called before any update(k,v) or combine(e) or getForClient().
 *
 *
 * A BatchScan will run a reducer on every tablet that sends results to the client.
 * combine() is called at the client to combine results from each one.
 */
interface Reducer {

  /**

   * @param options Options from the client.
   * *
   * @param env Passed from Accumulo. Null if created at the client.
   */
  @Throws(IOException::class)
  fun init(options: Map<String, String>, env: IteratorEnvironment)


  /** Reset reducer to its "zero" state, as if it was just init'd. Called at seek() and next() of iterators.  */
  @Throws(IOException::class)
  fun reset()


  /** Update internal state with a single entry.  */
  fun update(k: Key, v: Value)

  /** Update internal state with a collection of entries.  */
  fun combine(another: ByteArray)

  /** Signals whether the reducer has an element ready to serialize and send to the client.
   * Should always be false after reset() is called.
   */
  fun hasTopForClient(): Boolean

  /**
   * Emit the serialized state.
   * Returns null if no value should be emitted (indicates the "zero" state).
   * This MUST return null after reset() is called and before any update() or combine() methods are called.
   */
  val forClient: ByteArray?
}
