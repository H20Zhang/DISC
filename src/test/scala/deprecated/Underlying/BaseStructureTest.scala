package deprecated.Underlying

import org.apache.spark.adj.deprecated.execution.rdd._
import org.apache.spark.adj.deprecated.utlis.TestUtil
import org.scalatest.FunSuite

class BaseStructureTest extends FunSuite{

  test("KeyMapping"){


    val map1 = Map((0,1),(1,2),(2,3))
    val map2 = Map((1,2),(2,3))
    val listMap = List(1,2,3)
    val map3 = Map((1,0),(2,1),(3,2))

    val keyMapping1 = KeyMapping(map1)
    val keyMapping2 = KeyMapping(map2)
    val keyMapping3 = KeyMapping(listMap)
    val keyMapping4 = KeyMapping(map3)

    assert(TestUtil.listEqual(List(0,1,2), keyMapping1.getKeys().toSeq), "getKeys not passed")
    assert(TestUtil.listEqual(List(1,2,3), keyMapping1.getValues().toSeq), "getKeys not passed")
    assert(keyMapping1.getSubKeyMapping(List(1,2)).equals(keyMapping2),"getSubKeyMapping failed")
    assert(keyMapping1.toReverseMapping().equals(keyMapping4), "toReverseMapping failed")
    assert(TestUtil.listEqual(listMap,keyMapping3.toListMapping()),"toListMapping failed")
  }

  test("PatternInstance"){

    val list1 = List(1,2,3)
    val list2 = List(4)
    val lKeyMapping = KeyMapping(Seq(0,1,2))
    val rKeyMapping = KeyMapping(Seq(3))

    val pattern1 = PatternInstance(list1)
    val pattern2 = PatternInstance(list2)


    val totalNodes = (lKeyMapping.values ++ rKeyMapping.values).toSeq.max + 1
    val pattern3 = PatternInstance.slowBuild(pattern1,lKeyMapping,pattern2,rKeyMapping,totalNodes)

    assert(TestUtil.listEqual(PatternInstance(Seq(1,2,3,4)).pattern,pattern3.pattern),"build failed")

  }


  test("compactListTestOne"){

    val list1 = List(0)
    val list2 = List(1)
    val list3 = List(3)
    val list4 = List(4)
    val list = List(list1,list2,list3,list4)

    val compactList = CompactListBuilder(list,1)

    compactList.iterator.foreach{f =>
      val s = f.asInstanceOf[OneValuePatternInstance];
      println(s"${s.node1}")}
  }

  test("compactListTestTwo"){

    val list1 = List(0,1)
    val list2 = List(1,2)
    val list3 = List(3,4)
    val list4 = List(4,5)
    val list = List(list1,list2,list3,list4)

    val compactList = CompactListBuilder(list,2)

    compactList.iterator.foreach{f =>
      val s = f.asInstanceOf[TwoValuePatternInstance];
      println(s"${s.node1} ${s.node2}")}
  }

  test("compactListTestMany"){

    val list1 = List(0,1,3)
    val list2 = List(1,2,5)
    val list3 = List(3,4,7)
    val list4 = List(4,5,8)
    val list = List(list1,list2,list3,list4)

    val compactList = CompactListBuilder(list,3)

    compactList.iterator.foreach{f =>
      val s = f.asInstanceOf[EnumeratePatternInstance];
      s.pattern.foreach(print)
      println()
    }
  }

}
