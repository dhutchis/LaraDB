/**
 * A collection of functions to encode and decode ByteBuffers whose length may vary.
 * The main methods are `escapeAndJoin` and `splitAndUnescape`.
 *
 *
 * Idea: make the separator and/or escape character variable.
 * Place them at the beginning of the string.
 * Then we could choose characters that appear infrequently in the list of ByteBuffers to `escapeAndJoin`.
 */
package edu.washington.cs.laragraphulo.encoding

import org.apache.accumulo.core.util.ByteBufferUtil
import java.nio.ByteBuffer
import java.util.*



  const val sep: Byte = 0x00
  const val esc: Byte = 0x01

  fun escape(arr: ByteArray): ByteBuffer =
      escape(arr, 0, arr.size)

  fun escape(arr: ByteArray, bb: ByteBuffer): ByteBuffer =
      escape(arr, 0, arr.size, bb)

  fun escape(arr: ByteArray, off: Int, len: Int): ByteBuffer {
    val bb = escape(arr, off, len, ByteBuffer.allocate(len * 2))
    bb.flip()
    return bb
  }

  /** Change 'esc' -> 'escesc'; 'sep' -> 'escsep'.
   * Modifies `bb`. Assumes that `bb` has enough remaining capacity.
   */
  fun escape(arr: ByteArray, off: Int, len: Int, bb: ByteBuffer): ByteBuffer {
    var i = off
    var start = off
    while (i < off + len) {
      when (arr[i]) {
        esc -> {
          bb.put(arr, start, i - start)
              .put(esc).put(esc)
          start = i + 1
        }
        sep -> {
          bb.put(arr, start, i - start)
              .put(esc).put(sep)
          start = i + 1
        }
      }
      i++
    }
    bb.put(arr, start, i - start)
    return bb
  }

  /**
   * @param[bborig] The data to escape. Does not change the position of `bborig`.
   */
  fun escape(bborig: ByteBuffer): ByteBuffer {
    if (bborig.hasArray()) {
      return escape(bborig.array(), bborig.arrayOffset() + bborig.position(), bborig.remaining())
    } else {
      return escape(ByteBufferUtil.toBytes(bborig))
    }
  }

  /**
   * @param[bborig] The data to escape. Does not change the position of `bborig`.
   * @param[bb] The ByteBuffer to place newly escaped data in. Assumes it has enough remaining capacity.
   *            Does not flip `bb`.
   * @return `bb`
   */
  fun escape(bborig: ByteBuffer, bb: ByteBuffer): ByteBuffer {
    if (bborig.hasArray()) {
      return escape(bborig.array(), bborig.arrayOffset() + bborig.position(), bborig.remaining(), bb)
    } else {
      return escape(ByteBufferUtil.toBytes(bborig), bb)
    }
  }

  fun unescape(arr: ByteArray): ByteBuffer {
    return unescape(arr, 0, arr.size)
  }

  fun unescape(arr: ByteArray, off: Int, len: Int): ByteBuffer {
    val bb = unescape(arr, off, len, ByteBuffer.allocate(len))
    bb.flip()
    return bb
  }

  fun unescape(arr: ByteArray, off: Int, len: Int, bb: ByteBuffer): ByteBuffer {
    var i = off
    while (i < off + len) {
      val b = arr[i]
      if (b == esc) {
        bb.put(arr[i + 1])
        i += 2
      } else {
        bb.put(b)
        i++
      }
    }
    return bb
  }

  /** Does not change the position of `bborig`. */
  fun unescape(bborig: ByteBuffer): ByteBuffer {
    if (bborig.hasArray()) {
      return unescape(bborig.array(), bborig.arrayOffset() + bborig.position(), bborig.remaining())
    } else {
      return unescape(ByteBufferUtil.toBytes(bborig))
    }
  }

  val EMPTY_BUFFER: ByteBuffer = ByteBuffer.allocate(0)

  /**
   * Assuming 0 is the separator and 1 is the escape,
   * `[ 5022, 3, '', 71 ] -> 510220300711`.
   */
  fun escapeAndJoin(arrList: List<ByteBuffer>): ByteBuffer {
    if (arrList.isEmpty()) return EMPTY_BUFFER
    val bb = ByteBuffer.allocate(arrList.sumBy { 2 * it.remaining() + 1 } - 1) // a|b|c needs two chars of '|', not 3
    val iter = arrList.iterator()
    while (true) {
      val blist = iter.next()
      escape(blist, bb) // modifies bb
      if (iter.hasNext())
        bb.put(sep)
      else break
    }
    bb.flip()
    return bb
  }


  fun splitAndUnescape(arr: ByteArray): List<ByteBuffer> =
      splitAndUnescape(arr, 0, arr.size)


  /**
   * Assuming 0 is the separator and 1 is the escape,
   * `510220300711 -> [ 5022, 3, '', 71 ]`.
   */
  fun splitAndUnescape(arr: ByteArray, off: Int, len: Int): List<ByteBuffer> {
    assert(len == 0 ||
        len == 1 && arr[off] != esc ||
        len > 1 && (arr[off + len - 1] != esc || arr[off + len - 2] == esc)
        , { "Bad input array; last byte is an unescaped escape byte " + esc })
    var bb = ByteBuffer.allocate(len)
    val list: ArrayList<ByteBuffer> = arrayListOf()

    var start = off
    var i = off
    while (i < off + len) {
      when (arr[i]) {
        esc -> {
          bb.put(arr, start, i - start)
          i++
          start = i
        }
        sep -> {
          bb.put(arr, start, i - start)
          start = i + 1
          val bbnew = bb.slice()
          bb.flip()
          list.add(bb)
          bb = bbnew
        }
      }
      i++
    }
    bb.put(arr, start, i - start)
    bb.flip()
    list.add(bb)
    return list
  }
