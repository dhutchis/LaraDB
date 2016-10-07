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
      val name: String,
      val query: String,
      val catalog: Scheme,
      val data: Relation,
      val expected: Relation
  ) {
    override fun toString(): String = name
  }

  @Test
  fun test1() {
    println("Test: ${params.name}")
    println("query: ${params.query}")
    val ptree = StringReader(params.query).use { PTree.parseRaco(it) }
    println("ptree: $ptree")
    val racoOp = RacoOperator.parsePTreeToRacoTree(ptree)
    println("racoOp: $racoOp")
    println()
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

dhutchis@denine99:~/GITdir/raco/examples$ echo "A = scan(smallGraph); B = select src, dst from A; C = select dst from B; dump(C);" > tmp.myl && c:/anaconda/python.exe "C:\Users\Class2014\GITdir\raco\scripts\myrial" tmp.myl -l
Sequence
    Dump()[Apply(dst=dst)[Apply(src=src,dst=dst)[Scan(public:adhoc:smallGraph)]]


    dhutchis@denine99:~/GITdir/raco/examples$ echo "A = scan(smallGraph); B = selec
t src, dst from A; C = select dst from B; dump(C);" > tmp.myl && c:/anaconda/py
thon.exe "C:\Users\Class2014\GITdir\raco\scripts\myrial" tmp.myl -l -r
Sequence([Dump(Apply([('dst', NamedAttributeRef('dst'))], Apply([('src', NamedAt
tributeRef('src')), ('dst', NamedAttributeRef('dst'))], Scan(RelationKey('public
','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 1
0000, RepresentationProperties(frozenset([]), None, None)))))])

]

     */
    val tests = arrayOf(
//        Params(
//            query = "",
//            catalog = mapOf(),
//            data = listOf(),
//            expected = listOf()
//        ),
        /*
query: Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))
ptree: PNode(name=Dump, args=[PNode(name=Apply, args=[PList(list=[PPair(left=PString(str=src), right=PNode(name=NamedAttributeRef, args=[PString(str=src)])), PPair(left=PString(str=dst), right=PNode(name=NamedAttributeRef, args=[PString(str=dst)]))]), PNode(name=FileScan, args=[PString(str=mock.csv), PString(str=CSV), PNode(name=Scheme, args=[PList(list=[PPair(left=PString(str=src), right=PString(str=LONG_TYPE)), PPair(left=PString(str=dst), right=PString(str=LONG_TYPE))])]), PMap(map={})])])])
racoOp: Dump(input=Apply(emitters=[(src, NamedAttributeRef(attributename=src)), (dst, NamedAttributeRef(attributename=dst))], input=FileScan(file=mock.csv, format=CSV, scheme=[(src, LONG), (dst, LONG)], options={})))

         */
        Params(
            name = "dump apply filescan",
            query = "Dump(Apply([('src', NamedAttributeRef('src')), ('dst', NamedAttributeRef('dst'))], " +
                "FileScan('mock.csv', 'CSV', Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), {})))",
            catalog = listOf("src" to RacoType.LONG, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        ),
        /*
query: Store(RelationKey('public','adhoc','newtable'), Apply([('dst', NamedAttributeRef('dst'))], Scan(RelationKey('public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, RepresentationProperties(frozenset([]), None, None))))
ptree: PNode(name=Store, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=newtable)]), PNode(name=Apply, args=[PList(list=[PPair(left=PString(str=dst), right=PNode(name=NamedAttributeRef, args=[PString(str=dst)]))]), PNode(name=Scan, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=smallGraph)]), PNode(name=Scheme, args=[PList(list=[PPair(left=PString(str=src), right=PString(str=LONG_TYPE)), PPair(left=PString(str=dst), right=PString(str=LONG_TYPE))])]), PLong(v=10000), PNode(name=RepresentationProperties, args=[PNode(name=frozenset, args=[PList(list=[])]), PNone, PNone])])])])
racoOp: Store(relationKey=RelationKey(user=public, program=adhoc, relation=newtable), input=Apply(emitters=[(dst, NamedAttributeRef(attributename=dst))], input=Scan(relationKey=RelationKey(user=public, program=adhoc, relation=smallGraph), scheme=[(src, LONG), (dst, LONG)], cardinality=10000, partitioning=RepresentationProperties(hashPartition=[], sorted=[], grouped=[]))))

         */
        Params(
            name = "store apply scan Named",
            query = "Store(RelationKey('public','adhoc','newtable'), " +
                "Apply([('dst', NamedAttributeRef('dst'))], " +
                "Scan(RelationKey('public','adhoc','smallGraph'), " +
                "Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, " +
                "RepresentationProperties(frozenset([]), None, None))))",
            catalog = listOf("src" to RacoType.LONG, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        ),
/*
query: Store(RelationKey('public','adhoc','newtable'), Apply([('dst', UnnamedAttributeRef(1, None))], Scan(RelationKey('public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, RepresentationProperties(frozenset([]), None, None))))
ptree: PNode(name=Store, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=newtable)]), PNode(name=Apply, args=[PList(list=[PPair(left=PString(str=dst), right=PNode(name=UnnamedAttributeRef, args=[PLong(v=1), PNone]))]), PNode(name=Scan, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=smallGraph)]), PNode(name=Scheme, args=[PList(list=[PPair(left=PString(str=src), right=PString(str=LONG_TYPE)), PPair(left=PString(str=dst), right=PString(str=LONG_TYPE))])]), PLong(v=10000), PNode(name=RepresentationProperties, args=[PNode(name=frozenset, args=[PList(list=[])]), PNone, PNone])])])])
racoOp: Store(relationKey=RelationKey(user=public, program=adhoc, relation=newtable), input=Apply(emitters=[(dst, UnnamedAttributeRef(position=1, debug_info=null))], input=Scan(relationKey=RelationKey(user=public, program=adhoc, relation=smallGraph), scheme=[(src, LONG), (dst, LONG)], cardinality=10000, partitioning=RepresentationProperties(hashPartition=[], sorted=[], grouped=[]))))

 */
        Params(
            name = "store apply scan Unnamed",
            query = "Store(RelationKey('public','adhoc','newtable'), " +
                "Apply([('dst', UnnamedAttributeRef(1, None))], " +
                "Scan(RelationKey('public','adhoc','smallGraph'), " +
                "Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, " +
                "RepresentationProperties(frozenset([]), None, None))))",
            catalog = listOf("src" to RacoType.LONG, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        )
//        ,
//        Params(
//            name = "store apply scan Unnamed",
//            query = "Store(RelationKey('public','adhoc','newtable'), " +
//                "Apply([('dst', UnnamedAttributeRef(1, None))], " +
//                "Scan(RelationKey('public','adhoc','smallGraph'), " +
//                "Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, " +
//                "RepresentationProperties(frozenset([]), None, None))))",
//            catalog = listOf("src" to RacoType.LONG, "dst" to RacoType.LONG),
//            data = listOf(
//                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
//            ),
//            expected = listOf()
//        )
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