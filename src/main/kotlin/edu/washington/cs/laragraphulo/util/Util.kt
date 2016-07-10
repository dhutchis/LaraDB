package edu.washington.cs.laragraphulo.util

import com.google.common.base.Preconditions
import org.apache.accumulo.core.client.*
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.*
import org.apache.accumulo.core.iterators.IteratorUtil
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparator
import org.apache.log4j.LogManager
import java.nio.charset.StandardCharsets
import java.util.*

data class SeekData (
    val range: Range,
    val columnFamilies: Collection<ByteSequence>,
    val inclusive: Boolean
) {
  companion object {
    val ALL = SeekData(Range(), setOf(), false)
  }
}


/**
 * Utility functions
 */
object GraphuloUtil {
  private val log = LogManager.getLogger(GraphuloUtil::class.java)

  val DEFAULT_SEP_D4M_STRING = '\t'
  private val EMPTY_TEXT = Text()
  val OPT_SUFFIX = ".opt."


  /* Motivation for using -1 argument in String.split() call:
System.out.println(",".split(",",-1 ).length + Arrays.toString(",".split(",",-1 )));
System.out.println(",".split(",",0  ).length + Arrays.toString(",".split(",",0  )));
System.out.println("a,".split(",",-1).length + Arrays.toString("a,".split(",",-1)));
System.out.println("a,".split(",",0 ).length + Arrays.toString("a,".split(",",0 )));
System.out.println();
System.out.println(",".split(",",-1 )[1].length);
System.out.println("a,,".split(",",0 ).length + Arrays.toString("a,,".split(",",0 )));
System.out.println("a,,".split(",",-1).length + Arrays.toString("a,,".split(",",-1)));
System.out.println(",a,,".split(",",0 ).length + Arrays.toString(",a,,".split(",",0 )));
System.out.println(",a,,".split(",",-1).length + Arrays.toString(",a,,".split(",",-1)));
   */

  /**
   * Split a D4M String into each component. Does nothing special with ranges, i.e. the ':' character.
   */
  fun splitD4mString(str: String?): Array<String> {
    // maybe optimize away since this is a lower-level function
    Preconditions.checkArgument(str != null && !str.isEmpty(), "%s must be length at least 1", str)
    return str!!.substring(0, str.length - 1).split(Character.toString(str[str.length - 1]).toRegex()).toTypedArray()
  }

  /**
   * Split options on period characters.
   * "" holds entries without a period.

   * @return Map Prefix -> (Map entryWithoutPrefix -> value)
   */
  fun splitMapPrefix(options: Map<String, String>): Map<String, Map<String, String>> {
    val prefixMap = HashMap<String, MutableMap<String, String>>()
    for (entry in options.entries) {
      val key = entry.key
      val periodIdx = key.indexOf(".")
      if (periodIdx == 0)
        throw IllegalArgumentException("not allowed to have option that starts with period: " + entry)
      val prefix = if (periodIdx == -1)
        ""
      else
        key.substring(0, periodIdx)
      val afterPrefix = if (periodIdx == -1)
        key
      else
        key.substring(periodIdx + 1) // ok if empty

      var mapInside: MutableMap<String, String>? = prefixMap[prefix]
      if (mapInside == null) {
        mapInside = HashMap<String, String>()
      }
      mapInside.put(afterPrefix, entry.value)
      prefixMap.put(prefix, mapInside)
    }
    return prefixMap
  }

  fun preprendPrefixToKey(prefix: String, options: Map<String, String>): Map<String, String> {
    val res = HashMap<String, String>(options.size)
    for ((key, value) in options) {
      res.put(prefix + key, value)
    }
    return res
  }

  private val SETINFRNG: SortedSet<Range>

  init {
    val tmp = TreeSet<Range>()
    tmp.add(Range())
    SETINFRNG = Collections.unmodifiableSortedSet(tmp)
  }





//  /**
//   * Convert D4M string representation of rows to Ranges.
//   * Last character in the string is an arbitrary separator char
//   * that must not appear in the rows. The ':' cannot appear in rows either.
//   * See UtilTest for more test cases.
//   * Does not merge overlapping ranges.
//
//   * @param rowStr Ex: ':,r1,r3,r5,:,r7,r9,:,'
//   * *
//   * @return Ex: (-Inf,r1] [r3,r3) [r5,r7] [r9,+Inf)
//   */
//  /**
//   * @see .d4mRowToRanges
//   * @param singletonsArePrefix If true, then singleton entries in the D4M string are
//   * *                            made into prefix ranges instead of single row ranges.
//   */
//  @JvmOverloads fun d4mRowToRanges(rowStr: String?, singletonsArePrefix: Boolean = false): SortedSet<Range> {
//    if (rowStr == null || rowStr.isEmpty())
//      return TreeSet()
//    // could write my own version that does not do regex, but probably not worth optimizing
//    val rowStrSplit = splitD4mString(rowStr)
//    //if (rowStrSplit.length == 1)
//    val rowStrList = Arrays.asList(*rowStrSplit)
//    val pi = PeekingIterator3(rowStrList.iterator())
//    val rngset = TreeSet<Range>()
//
//    if (pi.peekFirst().equals(":")) { // (-Inf,
//      if (pi.peekSecond() == null) {
//        return SETINFRNG // (-Inf,+Inf)
//      } else {
//        if (pi.peekSecond().equals(":") || pi.peekThird() != null && pi.peekThird().equals(":"))
//          throw IllegalArgumentException("Bad D4M rowStr: " + rowStr)
//        rngset.add(Range(null, false, pi.peekSecond(), true)) // (-Inf,2]
//        pi.next()
//        pi.next()
//      }
//    }
//
//    while (pi.hasNext()) {
//      if (pi.peekSecond() == null) { // last singleton row [1,1~)
//        if (singletonsArePrefix)
//          rngset.add(Range.prefix(pi.peekFirst()))
//        else
//          rngset.add(Range.exact(pi.peekFirst()))
//        return rngset
//      } else if (pi.peekSecond().equals(":")) {
//        if (pi.peekThird() == null) { // [1,+Inf)
//          rngset.add(Range(pi.peekFirst(), true, null, false))
//          return rngset
//        } else { // [1,3]
//          if (pi.peekThird().equals(":"))
//            throw IllegalArgumentException("Bad D4M rowStr: " + rowStr)
//          rngset.add(Range(pi.peekFirst(), true, pi.peekThird(), true))
//          pi.next()
//          pi.next()
//          pi.next()
//        }
//      } else { // [1,1~)
//        if (singletonsArePrefix)
//          rngset.add(Range.prefix(pi.peekFirst()))
//        else
//          rngset.add(Range.exact(pi.peekFirst()))
//        pi.next()
//      }
//    }
//    return rngset
//  }

//  /**
//   * @see .d4mRowToRanges
//   * @param singletonsArePrefix If true, then singleton entries in the D4M string are
//   * *                            made into prefix ranges instead of single row ranges.
//   * *
//   * @return [ImmutableRangeSet]
//   */
//  fun d4mRowToGuavaRangeSet(rowStr: String?, singletonsArePrefix: Boolean): RangeSet<ByteSequence> {
//    if (rowStr == null || rowStr.isEmpty())
//      return ImmutableRangeSet.of<ByteSequence>()
//    // could write my own version that does not do regex, but probably not worth optimizing
//    val rowStrSplit = splitD4mString(rowStr)
//    //if (rowStrSplit.length == 1)
//    val rowStrList = Arrays.asList(*rowStrSplit)
//    val pi = PeekingIterator3(rowStrList.iterator())
//    val rngset = ImmutableRangeSet.builder<ByteSequence>()
//
//    if (pi.peekFirst().equals(":")) { // (-Inf,
//      if (pi.peekSecond() == null) {
//        return ImmutableRangeSet.of(com.google.common.collect.Range.all<ByteSequence>()) // (-Inf,+Inf)
//      } else {
//        if (pi.peekSecond().equals(":") || pi.peekThird() != null && pi.peekThird().equals(":"))
//          throw IllegalArgumentException("Bad D4M rowStr: " + rowStr)
//        rngset.add(com.google.common.collect.Range.atMost(ArrayByteSequence(pi.peekSecond()))) // (-Inf,2]
//        pi.next()
//        pi.next()
//      }
//    }
//
//    while (pi.hasNext()) {
//      if (pi.peekSecond() == null) { // last singleton row [1,1~)
//        if (singletonsArePrefix)
//          rngset.add(com.google.common.collect.Range.closedOpen(
//              ArrayByteSequence(pi.peekFirst()), ArrayByteSequence(Range.followingPrefix(Text(pi.peekFirst())).toString())))
//        else
//          rngset.add(com.google.common.collect.Range.singleton(ArrayByteSequence(pi.peekFirst())))
//        return rngset.build()
//      } else if (pi.peekSecond().equals(":")) {
//        if (pi.peekThird() == null) { // [1,+Inf)
//          rngset.add(com.google.common.collect.Range.atLeast(ArrayByteSequence(pi.peekFirst())))
//          return rngset.build()
//        } else { // [1,3]
//          if (pi.peekThird().equals(":"))
//            throw IllegalArgumentException("Bad D4M rowStr: " + rowStr)
//          rngset.add(com.google.common.collect.Range.closed(ArrayByteSequence(pi.peekFirst()), ArrayByteSequence(pi.peekThird())))
//          pi.next()
//          pi.next()
//          pi.next()
//        }
//      } else { // [1,1~)
//        if (singletonsArePrefix)
//          rngset.add(com.google.common.collect.Range.closedOpen(
//              ArrayByteSequence(pi.peekFirst()), ArrayByteSequence(Range.followingPrefix(Text(pi.peekFirst())).toString())))
//        else
//          rngset.add(com.google.common.collect.Range.singleton(ArrayByteSequence(pi.peekFirst())))
//        pi.next()
//      }
//    }
//    return rngset.build()
//  }

  @JvmOverloads fun rangesToD4MString(ranges: Collection<Range>?, sep: Char = DEFAULT_SEP_D4M_STRING): String {
    var ranges = ranges
    if (ranges == null || ranges.isEmpty())
      return ""
    ranges = Range.mergeOverlapping(ranges)
    val sb = StringBuilder()
    var infEnd: String? = null
    for (range in ranges!!) {
      if (range.isInfiniteStartKey && range.isInfiniteStopKey)
        return sb.append(':').append(sep).toString()
      else if (range.isInfiniteStartKey) {
        val endRow = normalizeEndRow(range)!!
        sb.insert(0, ":" + sep + endRow + sep)
      } else if (range.isInfiniteStopKey) {
        infEnd = normalizeStartRow(range)
        assert(infEnd != null)
      } else {
        val startRow = normalizeStartRow(range)
        val endRow = normalizeEndRow(range)
        assert(startRow != null && endRow != null)
        if (startRow == endRow)
          sb.append(startRow).append(sep)
        else
          sb.append(startRow).append(sep).append(':').append(sep).append(endRow).append(sep)
      }
    }
    if (infEnd != null)
      sb.append(infEnd).append(sep).append(':').append(sep)
    return sb.toString()
  }

  private fun normalizeStartRow(range: Range): String? {
    val startKey = range.startKey ?: return null
    val startRow = String(startKey.rowData.toArray(), StandardCharsets.UTF_8)
    if (!range.isStartKeyInclusive)
      return startRow + '\u0000'
    else
      return startRow
  }

  private fun normalizeEndRow(range: Range): String? {
    val endKey = range.endKey ?: return null
    val endRow = String(endKey.rowData.toArray(), StandardCharsets.UTF_8)
    if (!range.isEndKeyInclusive)
      return prevRow(endRow)
    else
      return endRow
  }

  val LAST_ONE_BYTE_CHAR = Character.toChars(java.lang.Byte.MAX_VALUE.toInt())[0]

  private fun prevRow(row: String): String =
      if (row[row.length - 1] == '\u0000')
        row.substring(0, row.length - 1)
      else
        row.substring(0, row.length - 1) + (row[row.length - 1].toInt() - 1).toChar() + LAST_ONE_BYTE_CHAR

  /**
   * Convert D4M string representation of individual rows/columns to Text objects.
   * No ':' allowed as an entire row name!
   * Last character in the string is an arbitrary separator char
   * that must not appear in the rows.
   * See UtilTest for more test cases.

   * @param rowStr Ex: 'a,b,c,d,'
   * *
   * @return A Text object for each one.
   */
  fun d4mRowToTexts(rowStr: String?): Collection<Text> {
    if (rowStr == null || rowStr.isEmpty())
      return emptySet()
    val rowStrSplit = splitD4mString(rowStr)
    val ts = HashSet<Text>(rowStrSplit.size)
    for (row in rowStrSplit) {
      if (row == ":")
        throw IllegalArgumentException("rowStr cannot contain the label \":\" ---- $rowStr")
      ts.add(Text(row))
    }
    return ts
  }

  @JvmOverloads fun textsToD4mString(texts: Collection<Text>?, sep: Char = DEFAULT_SEP_D4M_STRING): String {
    if (texts == null)
      return ""
    val sb = StringBuilder()
    for (text in texts)
      sb.append(text).append(sep)
    return sb.toString()
  }

  @JvmOverloads fun stringsToD4mString(texts: Collection<String>?, sep: Char = DEFAULT_SEP_D4M_STRING): String {
    if (texts == null)
      return ""
    val sb = StringBuilder()
    for (text in texts)
      sb.append(text).append(sep)
    return sb.toString()
  }

  fun textsToRanges(texts: Collection<Text>): Collection<Range> {
    val ranges = HashSet<Range>()
    for (text in texts)
      ranges.add(Range(text))
    return ranges
  }

  fun stringsToRanges(texts: Collection<String>): Collection<Range> {
    val ranges = HashSet<Range>()
    for (text in texts)
      ranges.add(Range(text))
    return ranges
  }

  val ONSCOPE_OPTION = "_ONSCOPE_"

  /**
   * Helper method for adding an option to an iterator
   * which OneTable and TwoTable Graphulo operations will interpret to limit the scopes an iterator applies to.
   * @param scopes Scopes to limit the iterator to. Only meaningful if not null, not empty, and not all.
   * *               Choices are from [org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope]: scan, minc, majc.
   * *
   * @return The same IteratorSetting as the one passed in.
   */
  fun addOnScopeOption(itset: IteratorSetting, scopes: EnumSet<IteratorUtil.IteratorScope>?): IteratorSetting {
    if (scopes == null || scopes.isEmpty() || scopes.size == IteratorUtil.IteratorScope.values().size)
      return itset
    var s = ""
    for (scope in scopes) {
      s += scope.name + ','
    }
    itset.addOption(ONSCOPE_OPTION, s)
    return itset
  }

  /**
   * Add the given Iterator to a table on scan, minc, majc scopes.
   * If already present on a scope, does not re-add the iterator to that scope.
   * Call it "plus".
   *
   *
   * Respects [.ONSCOPE_OPTION] if present from [.addOnScopeOption].
   */
  fun applyIteratorSoft(itset: IteratorSetting, tops: TableOperations, table: String) {
    // check for special option that limits iterator scope
    val scopeStrs = itset.options[ONSCOPE_OPTION]
    val scopesToConsider: EnumSet<IteratorUtil.IteratorScope>
    if (scopeStrs != null && !scopeStrs.isEmpty()) {
      scopesToConsider = EnumSet.noneOf(IteratorUtil.IteratorScope::class.java)
      for (scope in splitD4mString(scopeStrs))
        scopesToConsider.add(
            IteratorUtil.IteratorScope.valueOf(scope.toLowerCase()))
    } else
      scopesToConsider = EnumSet.allOf(IteratorUtil.IteratorScope::class.java)

    // checking if iterator already exists. Not checking for conflicts.
    var existing: IteratorSetting?
    val enumSet = EnumSet.noneOf(IteratorUtil.IteratorScope::class.java)
    for (scope in scopesToConsider) {
      existing = tops.getIteratorSetting(table, itset.name, scope)
      if (existing == null)
        enumSet.add(scope)
    }
    tops.attachIterator(table, itset, enumSet)
  }


  val EMPTY_BYTES = ByteArray(0)

  /**
   * Create a copy of key with all fields except the ones specified cleared.
   * @param key The key to copy
   * *
   * @param pk What fields to retain from the key
   * *
   * @return A new Key object pointing to new copies of fields specified by pk; other fields are empty/default.
   */
  fun keyCopy(key: Key, pk: PartialKey): Key =
      when (pk) {
        PartialKey.ROW -> Key(key.rowData.toArray(), EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, java.lang.Long.MAX_VALUE, false, true)
        PartialKey.ROW_COLFAM -> Key(key.rowData.toArray(), key.columnFamilyData.toArray(), EMPTY_BYTES, EMPTY_BYTES, java.lang.Long.MAX_VALUE, false, true)
        PartialKey.ROW_COLFAM_COLQUAL -> Key(key.rowData.toArray(), key.columnFamilyData.toArray(), key.columnQualifierData.toArray(), EMPTY_BYTES, java.lang.Long.MAX_VALUE, false, true)
        PartialKey.ROW_COLFAM_COLQUAL_COLVIS -> Key(key.rowData.toArray(), key.columnFamilyData.toArray(), key.columnQualifierData.toArray(), key.columnVisibilityData.toArray(), java.lang.Long.MAX_VALUE, false, true)
        PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME -> Key(key.rowData.toArray(), key.columnFamilyData.toArray(), key.columnQualifierData.toArray(), key.columnVisibilityData.toArray(), key.timestamp, false, true)
        PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL -> Key(key.rowData.toArray(), key.columnFamilyData.toArray(), key.columnQualifierData.toArray(), key.columnVisibilityData.toArray(), key.timestamp, key.isDeleted, true)
        else -> throw AssertionError("unknown pk: " + pk)
      }

//  /**
//   * Apply an appropriate column filter based on the input string.
//   * Four modes of operation:
//   * 1. Null or blank ("") `colFilter`: do nothing.
//   * 2. No ranges `colFilter`: use scanner.fetchColumn() which invokes an Accumulo system ColumnQualifierFilter.
//   * 3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
//   * 4. Multi-range `colFilter`: use Graphulo D4mRangeFilter.
//   * @param colFilter column filter string
//   * *
//   * @param scanner to call fetchColumn() on, for case #2; and addScanIterator(), for cases #3 and #4
//   * *
//   * @param priority to use for scan iterator setting, for case #3 and #4
//   */
//  //  @Deprecated
//  //   * @deprecated Use {@link #applyGeneralColumnFilter(String, ScannerBase, DynamicIteratorSetting)} for more robust filter setting.
//  fun applyGeneralColumnFilter(colFilter: String?, scanner: ScannerBase, priority: Int) {
//    //    System.err.println("colFilter: "+colFilter);
//    if (colFilter != null && !colFilter.isEmpty()) {
//      val pos1 = colFilter.indexOf(':')
//      if (pos1 == -1) { // no ranges - collection of singleton texts
//        for (text in GraphuloUtil.d4mRowToTexts(colFilter)) {
//          scanner.fetchColumn(GraphuloUtil.EMPTY_TEXT, text)
//        }
//      } else {
//        val ranges = GraphuloUtil.d4mRowToRanges(colFilter)
//        assert(ranges.size > 0)
//        val s: IteratorSetting
//        if (ranges.size == 1) { // single range - use ColumnSliceFilter
//          val r = ranges.first()
//          if (r.isInfiniteStartKey && r.isInfiniteStopKey)
//            return                // Infinite case: no filtering.
//          s = IteratorSetting(priority, ColumnSliceFilter::class.java)
//          //          System.err.println("start: "+(r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString())
//          //              +"end: "+(r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString()));
//          ColumnSliceFilter.setSlice(s, if (r.isInfiniteStartKey) null else r.startKey.row.toString(),
//              true, if (r.isInfiniteStopKey) null else r.endKey.row.toString(), true)
//          //          System.err.println("!ok "+GraphuloUtil.d4mRowToRanges(colFilter));
//        } else { // multiple ranges
//          //          System.err.println("ok "+GraphuloUtil.d4mRowToRanges(colFilter));
//          s = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter)
//        }
//        scanner.addScanIterator(s)
//      }
//    }
//  }
//
//  /**
//   * Apply an appropriate column filter based on the input string.
//   * Four modes of operation:
//   *
//   *  1. 1. Null or blank ("") `colFilter`: do nothing.
//   *  1. 2. No ranges `colFilter`: use scanner.fetchColumn() which invokes an Accumulo system [ColumnQualifierFilter].
//   *  1. 3. Singleton range `colFilter`: use Accumulo user [ColumnSliceFilter].
//   *  1. 4. Multi-range `colFilter`: use Graphulo [D4mRangeFilter].
//   *
//   * @param colFilter column filter string
//   * *
//   * @param scanner to call fetchColumn() on, for case #2
//   * *
//   * @param dis to call append()/prepend() on, for cases #3 and #4
//   * *
//   * @param append True means call [DynamicIteratorSetting.append]. False means call [DynamicIteratorSetting.prepend]
//   */
//  fun applyGeneralColumnFilter(colFilter: String?, scanner: ScannerBase, dis: DynamicIteratorSetting, append: Boolean) {
//    //    System.err.println("colFilter: "+colFilter);
//    if (colFilter != null && !colFilter.isEmpty()) {
//      val pos1 = colFilter.indexOf(':')
//      if (pos1 == -1) { // no ranges - collection of singleton texts
//        // todo - the order this filter applies is different. Ensure no logical bugs when we have case 2.
//        for (text in GraphuloUtil.d4mRowToTexts(colFilter)) {
//          scanner.fetchColumn(GraphuloUtil.EMPTY_TEXT, text)
//        }
//      } else {
//        val ranges = GraphuloUtil.d4mRowToRanges(colFilter)
//        assert(ranges.size > 0)
//        val s: IteratorSetting
//        if (ranges.size == 1) { // single range - use ColumnSliceFilter
//          val r = ranges.first()
//          s = IteratorSetting(1, ColumnSliceFilter::class.java)
//          //          System.err.println("start: "+(r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString())
//          //              +"end: "+(r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString()));
//          ColumnSliceFilter.setSlice(s, if (r.isInfiniteStartKey) null else r.startKey.row.toString(),
//              true, if (r.isInfiniteStopKey) null else r.endKey.row.toString(), true)
//          //          System.err.println("!ok "+GraphuloUtil.d4mRowToRanges(colFilter));
//        } else { // multiple ranges
//          //          System.err.println("ok "+GraphuloUtil.d4mRowToRanges(colFilter));
//          s = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter)
//        }
//        if (append)
//          dis.append(s)
//        else
//          dis.prepend(s)
//      }
//    }
//  }
//
//  /**
//   * For use within an iterator stack.
//   * Apply an appropriate column filter based on the input string.
//   * Four modes of operation:
//   * 1. Null or blank ("") `colFilter`: do nothing.
//   * 2. No ranges `colFilter`: use Accumulo system ColumnQualifierFilter.
//   * 3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
//   * 4. Multi-range `colFilter`: use Graphulo D4mRangeFilter.
//   * @param colFilter column filter string
//   * *
//   * @param skvi Parent / source iterator
//   * *
//   * @return SKVI with appropriate filter iterators placed in front of it.
//   */
//  @Throws(IOException::class)
//  fun applyGeneralColumnFilter(
//      colFilter: String?, skvi: SortedKeyValueIterator<Key, Value>, env: IteratorEnvironment): SortedKeyValueIterator<Key, Value> {
//    if (colFilter == null || colFilter.isEmpty())
//      return skvi
//
//    val pos1 = colFilter.indexOf(':')
//    if (pos1 == -1) { // no ranges - collection of singleton texts
//      val colset = HashSet<Column>()
//      for (text in GraphuloUtil.d4mRowToTexts(colFilter)) {
//        val by = text.copyBytes()
//        //        log.debug("Printing characters of string TEXT LIM: "+ Key.toPrintableString(by, 0, text.getLength(), 100));
//        //        log.debug("Printing characters of string TEXT    : "+ Key.toPrintableString(by, 0, by.length, 100));
//        colset.add(Column(EMPTY_BYTES, text.copyBytes(), EMPTY_BYTES))
//      }
//      return ColumnQualifierFilter(skvi, colset)
//
//    } else {
//      val ranges = GraphuloUtil.d4mRowToRanges(colFilter)
//      assert(ranges.size > 0)
//
//      if (ranges.size == 1) { // single range - use ColumnSliceFilter
//        val r = ranges.first()
//        val map = HashMap<String, String>()
//
//        val start = if (r.isInfiniteStartKey) null else r.startKey.row.toString()
//        val end = if (r.isInfiniteStopKey) null else r.endKey.row.toString()
//        val startInclusive = true
//        val endInclusive = true
//
//        if (start != null)
//          map.put(ColumnSliceFilter.START_BOUND, start)
//        if (end != null)
//          map.put(ColumnSliceFilter.END_BOUND, end)
//        map.put(ColumnSliceFilter.START_INCLUSIVE, startInclusive.toString())
//        map.put(ColumnSliceFilter.END_INCLUSIVE, endInclusive.toString())
//
//        val filter = ColumnSliceFilter()
//        filter.init(skvi, map, env)
//        return filter
//
//      } else { // multiple ranges
//        val filter = D4mRangeFilter()
//        filter.init(skvi, D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter).getOptions(), env)
//        return filter
//      }
//    }
//  }

  /**
   * Create a new instance of a class whose name is given, as a descendent of a given subclass.
   */
  fun <E> subclassNewInstance(classname: String, parentClass: Class<E>): E {
    val c = try {
      Class.forName(classname)
    } catch (e: ClassNotFoundException) {
      throw IllegalArgumentException("Can't find class: " + classname, e)
    }

    val cm = try {
      c.asSubclass(parentClass)
    } catch (e: ClassCastException) {
      throw IllegalArgumentException(classname + " is not a subclass of " + parentClass.name, e)
    }

    try {
      return cm.newInstance()
    } catch (e: Exception) { // InstantiationException or IllegalAccessException
      throw RuntimeException("can't instantiate new instance of " + cm.name, e)
    }
  }


  /** If str begins with prefix, return a String containing the characters after the prefix. Otherwise return null.  */
  fun stringAfter(prefix: ByteArray, str: ByteArray): String? =
      if (0 == WritableComparator.compareBytes(str, 0, prefix.size, prefix, 0, prefix.size))
        String(str, prefix.size, str.size - prefix.size, StandardCharsets.UTF_8)
      else
        null

//  /**
//   * Add Cartesian product of prefixes and suffixes to a string, each give as a D4M String.
//   * @see .padD4mString_Single
//   */
//  fun padD4mString(prefixes: String?, suffixes: String?, str: String): String {
//    var prefixes = prefixes
//    var suffixes = suffixes
//    var str = str
//    if (prefixes == null || prefixes.isEmpty())
//      prefixes = ","
//    if (suffixes == null || suffixes.isEmpty())
//      suffixes = ","
//    if (prefixes.length <= 1 && suffixes.length <= 1)
//      return str
//
//    if (d4mStringContainsRange(str)) {
//      //      if (suffixes.length()>1)
//      //        throw new UnsupportedOperationException("No way to append the suffixes "+suffixes+
//      //            " to a D4M String containing a Range: "+str);
//      // add prefix to v0 Ranges. Goto full Range Objects because ':' is complicated.
//      val tmp = GraphuloUtil.d4mRowToRanges(str)
//      val tmp2 = TreeSet<Range>()
//      for (startPre in GraphuloUtil.splitD4mString(prefixes))
//        for (range in tmp)
//          tmp2.add(GraphuloUtil.prependPrefixToRange(startPre, range))
//      str = GraphuloUtil.rangesToD4MString(tmp2, str[str.length - 1])
//      prefixes = ","
//    }
//
//    var s = ""
//    for (pre in GraphuloUtil.splitD4mString(prefixes)) {
//      for (suf in GraphuloUtil.splitD4mString(suffixes)) {
//        s += padD4mString_Single(pre, suf, str)
//      }
//    }
//    return s
//  }

  /**
   * Does the str contain the colon+separator, meaning it has a range?
   */
  fun d4mStringContainsRange(str: String): Boolean {
    val cont = ":" + str[str.length - 1]
    return str.contains(cont)
  }

  /** Add prefix and/or suffix to every part of a D4M string.
   * prependStartPrefix("pre|","X","a,b,:,v,:,") ==>
   * "pre|aX,pre|bX,:,pre|vX,:,"
   */
  fun padD4mString_Single(prefix: String?, suffix: String?, str: String): String {
    var prefix = prefix
    var suffix = suffix
    if (prefix == null)
      prefix = ""
    if (suffix == null)
      suffix = ""
    if (prefix.isEmpty() && suffix.isEmpty())
      return str
    val sep = str[str.length - 1]
    val sb = StringBuilder()
    for (part in GraphuloUtil.splitD4mString(str)) {
      if (part == ":")
        sb.append(part).append(sep)
      else
        sb.append(prefix).append(part).append(suffix).append(sep)
    }
    return sb.toString()
  }

  /**
   * Count the number of terms in a D4M String that do NOT have any ranges.
   * @param s D4M String
   * *
   * @return Number of terms in the D4M String.
   */
  fun numD4mStr(s: String?): Int {
    if (s == null || s.isEmpty())
      return 0
    Preconditions.checkArgument(!s.contains(":"), "Cannot count number of terms in a D4M String with a range: ", s)
    var cnt = -1
    val sep = s[s.length - 1]
    var pos = -1
    do {
      pos = s.indexOf(sep, pos+1)
      cnt++
    } while (pos != -1)
    return cnt
  }


  /**
   * Pad a range with a prefix, so the new range points to entries
   * that begin with the prefix and then satisfy the original range.
   * Only uses the Row field of the original Range; discards the rest.
   * @param pre The prefix
   * *
   * @param rold The original Range
   * *
   * @return New Range of the prefix plus the original
   */
  fun prependPrefixToRange(pre: String?, rold: Range): Range {
    if (pre == null || pre.isEmpty())
      return rold
    if (rold.isInfiniteStopKey && rold.isInfiniteStartKey)
      return Range.prefix(pre)
    if (rold.isInfiniteStartKey)
      return Range(pre, true, pre + normalizeEndRow(rold)!!, true)
    if (rold.isInfiniteStopKey)
      return Range(pre + normalizeStartRow(rold)!!, true,
          Range.followingPrefix(Text(pre)).toString(), false)
    return Range(pre + normalizeStartRow(rold)!!, true,
        pre + normalizeEndRow(rold)!!, true)
  }

  fun d4mStringContainsEmptyString(str: String?): Boolean {
    Preconditions.checkArgument(str != null && !str.isEmpty(), "%s is not a D4M String", str)
    if (str!!.length == 1)
      return true
    val sep = Character.toString(str[str.length - 1])
    val sepsep = sep + sep
    return str.contains(sepsep)

  }

//  /**
//   * Makes each input term into a prefix range.
//   *
//   * "v1,v5," => "v1|,:,v1},v5|,:,v5},"
//   * "v1,:,v3,v5," => "v1,:,v3,v5|,:,v5},"
//   *
//   */
//  fun singletonsAsPrefix(str: String): String {
//    Preconditions.checkNotNull(str)
//    Preconditions.checkArgument(!str.isEmpty())
//    //    Preconditions.checkArgument(str.indexOf(':') != -1, "Cannot have the ':' character: "+str);
//    val sep = str[str.length - 1]
//    if (d4mStringContainsEmptyString(str))
//    // empty prefix is full range.
//      return ":" + sep
//
//    if (!d4mStringContainsRange(str)) {
//      val sb = StringBuilder()
//      for (vktext in GraphuloUtil.splitD4mString(str)) {
//        sb.append(vktext).append(sep).append(':').append(sep).append(prevRow(Range.followingPrefix(Text(vktext)).toString())).append(sep)
//      }
//      return sb.toString()
//    }
//
//    val prefixRngs = d4mRowToRanges(str, true)
//    return rangesToD4MString(prefixRngs, sep)
//  }

  /**
   * Makes each input term into a prefix range.
   * "v1,v5," => "v1,:,v1\255,v5,:,v5\255,"
   */
  fun singletonsAsPrefix(vktexts: Collection<Text>, sep: Char): String {
    val sb = StringBuilder()
    for (vktext in vktexts) {
      sb.append(vktext.toString()).append(sep).append(':').append(sep).append(prevRow(Range.followingPrefix(Text(vktext)).toString())).append(sep)
    }
    return sb.toString()
  }


  fun copyTopEntry(skvi: SortedKeyValueIterator<Key, Value>): Map.Entry<Key, Value> {
    val k = keyCopy(skvi.topKey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL)
    val v = Value(skvi.topValue)
    return object : Map.Entry<Key, Value> {
      override val key = k
      override val value = v
    }
  }

  /**
   * Write entries to a table.
   */
  fun writeEntries(connector: Connector, map: Map<Key, Value>, table: String, createIfNotExist: Boolean) {
    if (createIfNotExist && !connector.tableOperations().exists(table))
      try {
        connector.tableOperations().create(table)
      } catch (e: AccumuloException) {
        log.error("trouble creating " + table, e)
        throw RuntimeException(e)
      } catch (e: AccumuloSecurityException) {
        log.error("trouble creating " + table, e)
        throw RuntimeException(e)
      } catch (e: TableExistsException) {
        log.error("crazy", e)
        throw RuntimeException(e)
      }

    val bwc = BatchWriterConfig()
    bwc.maxWriteThreads = 25
    val bw: BatchWriter
    try {
      bw = connector.createBatchWriter(table, bwc)
    } catch (e: TableNotFoundException) {
      log.error("tried to write to a non-existant table " + table, e)
      throw RuntimeException(e)
    }

    try {
      for ((k, value) in map) {
        val rowData = k.rowData
        val cfData = k.columnFamilyData
        val cqData = k.columnQualifierData
        val m = Mutation(rowData.toArray(), rowData.offset(), rowData.length())
        m.put(cfData.toArray(), cqData.toArray(), k.columnVisibilityParsed, value.get())
        bw.addMutation(m)
      }

    } catch (e: MutationsRejectedException) {
      log.error("mutations rejected", e)
      throw RuntimeException(e)
    } finally {
      try {
        bw.close()
      } catch (e: MutationsRejectedException) {
        log.error("mutations rejected while trying to close BatchWriter", e)
      }

    }
  }

  /** Copy the splits placed on table t1 to table t2.  */
  @Throws(AccumuloException::class)
  fun copySplits(tops: TableOperations, t1: String, t2: String) {
    try {
      val splits = tops.listSplits(t1)
      val ss = TreeSet(splits)
      tops.addSplits(t2, ss)
    } catch (e: TableNotFoundException) {
      log.error("cannot handle splits copying from $t1 to $t2", e)
      throw RuntimeException(e)
    } catch (e: AccumuloSecurityException) {
      log.error("cannot handle splits copying from $t1 to $t2", e)
      throw RuntimeException(e)
    }

  }

  /** Delete tables. If they already exist, delete and re-create them if forceDelete==true,
   * otherwise throw an IllegalStateException.  */
  fun deleteTables(connector: Connector, vararg tns: String) {
    val tops = connector.tableOperations()
    for (tn in tns) {
      if (tn != null && tops.exists(tn)) {
        try {
          tops.delete(tn)
        } catch (e: AccumuloException) {
          log.error("Problem deleing temporary table " + tn, e)
          throw RuntimeException(e)
        } catch (e: AccumuloSecurityException) {
          log.error("Problem deleing temporary table " + tn, e)
          throw RuntimeException(e)
        } catch (e: TableNotFoundException) {
          log.error("crazy", e)
          throw RuntimeException(e)
        }

      }
    }
  }

  /** Switches row and column qualifier. Returns HashMap.  */
  fun <V> transposeMap(mapOrig: MutableMap<Key, V>): Map<Key, V> {
    val m = HashMap<Key, V>(mapOrig.size)
    return transposeMapHelp<MutableMap<Key, V>, V>(mapOrig, m)
  }

  /** Switches row and column qualifier. Use same comparator as the given map. Returns TreeMap.  */
  fun <V> transposeMap(mapOrig: SortedMap<Key, V>): SortedMap<Key, V> {
    val m = TreeMap<Key, V>(mapOrig.comparator())
    return transposeMapHelp<SortedMap<Key, V>, V>(mapOrig, m)
  }

  private fun <M : MutableMap<Key, V>, V> transposeMapHelp(orig: Map<Key, V>, neww: M): M {
    for ((k0, value) in orig) {
      val k = Key(k0.columnQualifier, k0.columnFamily,
          k0.row, k0.columnVisibilityParsed, k0.timestamp)
      neww.put(k, value)
    }
    return neww
  }

//  /**
//   * Generates RemoteSourceIterator (possibly x2), TwoTableIterator, RemoteWriteIterator
//   * configuration through a DynamicIteratorSetting.
//   * @param map Map of all options.
//   * *
//   * @param priority Priority to use for the IteratorSetting of the whole stack
//   * *
//   * @param name Null means use the default name "TableMultIterator"
//   */
//  fun tableMultIterator(
//      map: Map<String, String>,
//      priority: Int, name: String?): IteratorSetting {
//    val optDM = HashMap<String, String>()
//    val optC = HashMap<String, String>()
//    run {
//      val prefixMap = GraphuloUtil.splitMapPrefix(map)
//      for ((prefix, entryMap) in prefixMap) {
//
//        when (prefix) {
//          TwoTableIterator.PREFIX_AT, TwoTableIterator.PREFIX_B -> optDM.putAll(GraphuloUtil.preprendPrefixToKey(prefix + '.', entryMap))
//          "C" -> optC.putAll(entryMap)
//          else -> for ((key, value) in entryMap) {
//            //              switch (entry.getKey()) {
//            //                case "dotmode":
//            //                case "multiplyOp":
//            //                  optDM.put(entry.getKey(), entry.getValue());
//            //                  break;
//            //                default:
//            //                  log.warn("Unrecognized option: " + prefix + '.' + entry);
//            //                  break;
//            //              }
//            if (prefix.isEmpty())
//              optDM.put(key, value)
//            else
//              optDM.put(prefix + '.' + key, value)
//          }
//        }
//      }
//    }
//    val dis = DynamicIteratorSetting(priority, name ?: "TableMultIterator").append(IteratorSetting(1, TwoTableIterator::class.java, optDM))
//    if (!optC.isEmpty())
//      dis.append(IteratorSetting(1, RemoteWriteIterator::class.java, optC))
//    return dis.toIteratorSetting()
//  }

}




