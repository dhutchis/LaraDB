package edu.washington.cs.laragraphulo.opt.raco

import org.apache.accumulo.core.data.ArrayByteSequence
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.StringReader
import java.nio.ByteBuffer
import edu.washington.cs.laragraphulo.opt.raco.PTree.*






@RunWith(Parameterized::class)
class RacoConvertTest(
    val params: Params
) {


  data class Params (
      val query: String,
      val catalog: Scheme,
      val data: Relation,
      val expected: Relation
  )

  @Test
  fun test1() {
    println("query: ${params.query}")
    val ptree = StringReader(params.query).use { PTree.parseRaco(it) }
    println("ptree: $ptree")
    val racoOp = RacoOperator.parsePTreeToRacoTree(ptree)
    println("racoOp: $racoOp")
  }


  companion object {


    fun Long.toABS() = ByteBuffer.allocate(8).putLong(this).array().let { ArrayByteSequence(it, 0, it.size) }

    // 'public:adhoc:smallGraph' : [('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')],
    // A = scan(smallGraph); B = select src, dst from A; dump(B);
    /*
    echo "A = scan(smallGraph); B = select src, dst from A; dump(B);" > tmp.myl &&
    c:/anaconda/python.exe "C:\Users\Class2014\GITdir\raco\scripts\myrial" tmp.myl -l
    Sequence
      Dump()[Apply(src=src,dst=dst)[Scan(public:adhoc:smallGraph)]]

    Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], Scan(RelationKey(
    'public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000,
    RepresentationProperties(frozenset([]), None, None))))

    list [
    tuple (
    map {

    A = load("mock.csv", csv(schema(src:int,dst:int))); B = select src, dst from A; dump(B);
    Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))],
    FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))



    This shows that Apply acts as Project. Look at the -L version.

    dhutchis@denine99:~/GITdir/raco/examples$ echo "A = scan(smallGraph); B = selec
t src, dst from A; C = select dst from B; dump(C);" > tmp.myl &&     c:/anacond
a/python.exe "C:\Users\Class2014\GITdir\raco\scripts\myrial" tmp.myl -L
Dump()[Apply(dst=$1)[Scan(public:adhoc:smallGraph)]]

dhutchis@denine99:~/GITdir/raco/examples$ echo "A = scan(smallGraph); B = selec
t src, dst from A; C = select dst from B; dump(C);" > tmp.myl &&     c:/anacond
a/python.exe "C:\Users\Class2014\GITdir\raco\scripts\myrial" tmp.myl -l
Sequence
    Dump()[Apply(dst=dst)[Apply(src=src,dst=dst)[Scan(public:adhoc:smallGraph)]]
]

     */
    val tests = arrayOf(
//        Params(
//            query = "",
//            catalog = mapOf(),
//            data = listOf(),
//            expected = listOf()
//        ),
        Params(
            query = "Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], " +
                "FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))",
            catalog = listOf("src" to RacoType.LONG, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        )
//        ,
//        Params(
//            query = "",
//            catalog = mapOf("src" to RacoType.LONG, "dst" to RacoType.LONG),
//            data = listOf(
//                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS()),
//                mapOf("src" to 1L.toABS(), "dst" to 3L.toABS()),
//                mapOf("src" to 1L.toABS(), "dst" to 4L.toABS()),
//                mapOf("src" to 2L.toABS(), "dst" to 3L.toABS()),
//                mapOf("src" to 2L.toABS(), "dst" to 5L.toABS()),
//                mapOf("src" to 3L.toABS(), "dst" to 4L.toABS())
//            ),
//            expected = listOf()
//        )
    )

    @JvmStatic
    @Parameterized.Parameters(name = "test {index}: {0}")
    fun parameters(): Array<out Any> = tests
  }



}