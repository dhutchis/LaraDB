package edu.washington.cs.laragraphulo.opt.raco

import edu.washington.cs.laragraphulo.opt.*
import org.apache.accumulo.core.data.ArrayByteSequence
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.StringReader
import java.nio.ByteBuffer
import edu.washington.cs.laragraphulo.opt.raco.PTree.*
import edu.washington.cs.laragraphulo.opt.viz.generateDot
import org.junit.Assume


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
  fun test() {
    println("Test: ${params.name}")
    println("query: ${params.query}")
    // testConvert()
    val ptree = StringReader(params.query).use { PTree.parseRaco(it) }
    println("ptree: $ptree")
    val racoOp = RacoOperator.parsePTreeToRacoTree(ptree)
    println("racoOp: $racoOp")

    if (params.name == "dump apply filescan")
      Assume.assumeFalse("Dump is not implemented; skipping the compile part of this test", true)

    // testCompile()
    val callables = executorsRacoOnAccumulo(racoOp, hardcodedAccumuloConfig)
    println("Callables : $callables")

    // testSerialize()
    callables.forEachIndexed { i, callable ->
      print("$i: ")
      @Suppress("UNCHECKED_CAST")
      when (callable) {
        is CreateTableTask -> {
          println("CreateTableTask(${callable.tableName})")
        }
        is AccumuloPipelineTask<*> -> {
          val table = callable.pipeline.tableName
          val serializer = callable.pipeline.serializer
          val skvi = callable.pipeline.data
          skvi as Op<SKVI>
          serializer as Serializer<Op<SKVI>,Op<SKVI>>

          println("AccumuloPipelineTask($table, ${callable.pipeline.scanRange}): $skvi")
          println("dot:\n${skvi.generateDot()}")


          val serialized = serializer.serializeToString(skvi)
          val deserialized = serializer.deserializeFromString(serialized)
//          Assert.assertEquals("Serialization should match original object", callable.pipeline.data, deserialized)
        }
        else -> {
          println("???: $callable")
        }
      }
    }
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
Test: store apply scan Named
query: Store(RelationKey('public','adhoc','newtable'), Apply([('dst', NamedAttributeRef('dst'))], Scan(RelationKey('public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, RepresentationProperties(frozenset([]), None, None))))
ptree: PNode(name=Store, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=newtable)]), PNode(name=Apply, args=[PList(list=[PPair(left=PString(str=dst), right=PNode(name=NamedAttributeRef, args=[PString(str=dst)]))]), PNode(name=Scan, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=smallGraph)]), PNode(name=Scheme, args=[PList(list=[PPair(left=PString(str=src), right=PString(str=LONG_TYPE)), PPair(left=PString(str=dst), right=PString(str=LONG_TYPE))])]), PLong(v=10000), PNode(name=RepresentationProperties, args=[PNode(name=frozenset, args=[PList(list=[])]), PNone, PNone])])])])
racoOp: Store(relationKey=RelationKey(user=public, program=adhoc, relation=newtable), input=Apply(emitters=[(dst, NamedAttributeRef(attributename=dst))], input=Scan(relationKey=RelationKey(user=public, program=adhoc, relation=smallGraph), scheme=[(src, LONG), (dst, LONG)], cardinality=10000, partitioning=RepresentationProperties(hashPartition=[], sorted=[], grouped=[]))))
Callables : [CreateTableTask(tableName=public_adhoc_newtable, accumuloConfig=AccumuloConfig(instance=instance), ntc=org.apache.accumulo.core.client.admin.NewTableConfiguration@475530b9), AccumuloPipelineTask(accumuloPipeline=AccumuloPipeline(op=OpRWI([OpKeyValueToSkviAdapter([OpTupleToKeyValueIterator([OpApplyIterator([OpAccumuloBase([AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2), AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2)]), [RefKey(tupleNum=0, keyNum=1)], RefFamily(tupleNum=0), []]), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0)])]), public_adhoc_newtable, AccumuloConfig(instance=instance)]), serializer=edu.washington.cs.laragraphulo.opt.RacoToAccumuloKt$opSerializer$1@4c70fda8, tableName=public_adhoc_smallGraph), accumuloConfig=AccumuloConfig(instance=instance))]
0: CreateTableTask(public_adhoc_newtable)
1: AccumuloPipelineTask(public_adhoc_smallGraph): OpRWI([OpKeyValueToSkviAdapter([OpTupleToKeyValueIterator([OpApplyIterator([OpAccumuloBase([AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2), AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2)]), [RefKey(tupleNum=0, keyNum=1)], RefFamily(tupleNum=0), []]), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0)])]), public_adhoc_newtable, AccumuloConfig(instance=instance)])

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
Test: store apply scan Unnamed
query: Store(RelationKey('public','adhoc','newtable'), Apply([('dst', UnnamedAttributeRef(1, None))], Scan(RelationKey('public','adhoc','smallGraph'), Scheme([('src', 'LONG_TYPE'), ('dst', 'LONG_TYPE')]), 10000, RepresentationProperties(frozenset([]), None, None))))
ptree: PNode(name=Store, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=newtable)]), PNode(name=Apply, args=[PList(list=[PPair(left=PString(str=dst), right=PNode(name=UnnamedAttributeRef, args=[PLong(v=1), PNone]))]), PNode(name=Scan, args=[PNode(name=RelationKey, args=[PString(str=public), PString(str=adhoc), PString(str=smallGraph)]), PNode(name=Scheme, args=[PList(list=[PPair(left=PString(str=src), right=PString(str=LONG_TYPE)), PPair(left=PString(str=dst), right=PString(str=LONG_TYPE))])]), PLong(v=10000), PNode(name=RepresentationProperties, args=[PNode(name=frozenset, args=[PList(list=[])]), PNone, PNone])])])])
racoOp: Store(relationKey=RelationKey(user=public, program=adhoc, relation=newtable), input=Apply(emitters=[(dst, UnnamedAttributeRef(position=1, debug_info=null))], input=Scan(relationKey=RelationKey(user=public, program=adhoc, relation=smallGraph), scheme=[(src, LONG), (dst, LONG)], cardinality=10000, partitioning=RepresentationProperties(hashPartition=[], sorted=[], grouped=[]))))
Callables : [CreateTableTask(tableName=public_adhoc_newtable, accumuloConfig=AccumuloConfig(instance=instance), ntc=org.apache.accumulo.core.client.admin.NewTableConfiguration@4445629), AccumuloPipelineTask(accumuloPipeline=AccumuloPipeline(op=OpRWI([OpKeyValueToSkviAdapter([OpTupleToKeyValueIterator([OpApplyIterator([OpAccumuloBase([AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2), AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2)]), [RefKey(tupleNum=0, keyNum=1)], RefFamily(tupleNum=0), []]), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0)])]), public_adhoc_newtable, AccumuloConfig(instance=instance)]), serializer=edu.washington.cs.laragraphulo.opt.RacoToAccumuloKt$opSerializer$1@4c70fda8, tableName=public_adhoc_smallGraph), accumuloConfig=AccumuloConfig(instance=instance))]
0: CreateTableTask(public_adhoc_newtable)
1: AccumuloPipelineTask(public_adhoc_smallGraph): OpRWI([OpKeyValueToSkviAdapter([OpTupleToKeyValueIterator([OpApplyIterator([OpAccumuloBase([AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2), AP(dap[<src@LONG@8>, <dst@LONG@8>]; lap=[]; val=[]; sort=2)]), [RefKey(tupleNum=0, keyNum=1)], RefFamily(tupleNum=0), []]), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0), AP(dap[<dst@LONG@8>]; lap=[]; val=[]; sort=0)])]), public_adhoc_newtable, AccumuloConfig(instance=instance)])

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
        ),


        Params(
            name = "store apply scan Unnamed with manual DAP",
            query = "Store(RelationKey('public','adhoc','newtable'), " +
                "Apply([('dst', UnnamedAttributeRef(1, None))], " +
                "Scan(RelationKey('public','adhoc','smallGraph'), " +
                "Scheme([('src', 'LONG_TYPE'), ('$__DAP__', 'STRING_TYPE'), ('dst', 'LONG_TYPE')]), 10000, " +
                "RepresentationProperties(frozenset([]), None, None))))",
            catalog = listOf("src" to RacoType.LONG, __DAP__ to RacoType.STRING, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        ),


        Params(
            name = "store apply scan Unnamed with manual DAP and __TS apply",
            query = "Store(RelationKey('public','adhoc','newtable'), " +
                "Apply([('dst', UnnamedAttributeRef(1, None)), ('dst__TS', NumericLiteral(42))], " +
                "Scan(RelationKey('public','adhoc','smallGraph'), " +
                "Scheme([('src', 'LONG_TYPE'), ('$__DAP__', 'STRING_TYPE'), ('dst', 'LONG_TYPE')]), 10000, " +
                "RepresentationProperties(frozenset([]), None, None))))",
            catalog = listOf("src" to RacoType.LONG, __DAP__ to RacoType.STRING, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        ),


        Params(
            name = "store apply scan Unnamed with manual DAP and __TS and __VIS apply",
            query = "Store(RelationKey('public','adhoc','newtable'), " +
                "Apply([('dst', UnnamedAttributeRef(1, None)), ('dst__TS', NumericLiteral(42)), ('dst__VIS', StringLiteral(''))], " +
                "Scan(RelationKey('public','adhoc','smallGraph'), " +
                "Scheme([('src', 'LONG_TYPE'), ('$__DAP__', 'STRING_TYPE'), ('dst', 'LONG_TYPE')]), 10000, " +
                "RepresentationProperties(frozenset([]), None, None))))",
            catalog = listOf("src" to RacoType.LONG, __DAP__ to RacoType.STRING, "dst" to RacoType.LONG),
            data = listOf(
                mapOf("src" to 1L.toABS(), "dst" to 2L.toABS())
            ),
            expected = listOf()
        ),


        Params(
            name = "testing raco federated parse",
            query =
              "FileStore('/home/dhutchis/gits/raco/raco/backends/federated/tests/V5407830105', 'CSV', {}, " +
                "Apply([" +
                    "('src_ip', NamedAttributeRef('SrcAddr')), " +
                    "('dst_ip', NamedAttributeRef('SrcAddr')), " +
                    "('value', NumericLiteral(1.0))], " +
                  "Select(GT(UnnamedAttributeRef(0, None), NumericLiteral(500)), " +
                    "Scan(RelationKey('public','adhoc','netflow'), " +
                      "Scheme([(u'TotBytes', 'INT_TYPE'), " +
                        "(u'StartTime', 'STRING_TYPE'), " +
                        "(u'SrcAddr', 'STRING_TYPE'), " +
                        "(u'DstAddr', 'STRING_TYPE'), " +
                        "(u'RATE', 'DOUBLE_TYPE'), " +
                        "(u'Dur', 'DOUBLE_TYPE'), " +
                        "(u'Dir', 'STRING_TYPE'), " +
                        "(u'Proto', 'STRING_TYPE'), " +
                        "(u'Sport', 'STRING_TYPE'), " +
                        "(u'Dport', 'STRING_TYPE'), " +
                        "(u'State', 'STRING_TYPE'), " +
                        "(u'sTos', 'LONG_TYPE'), " +
                        "(u'dTos', 'LONG_TYPE'), " +
                        "(u'TotPkts', 'LONG_TYPE'), " +
                        "(u'SrcBytes', 'LONG_TYPE'), " +
                        "(u'Label', 'STRING_TYPE')]), " +
                      "(<raco.backends.myria.catalog.MyriaCatalog object at 0x7f207134fbd0>, 7), " +
                      "RepresentationProperties(frozenset([]), None, None)" +
                  "))))",
            catalog = listOf(),
            data = listOf(),
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