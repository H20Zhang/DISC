import org.apache.spark.Logo.Physical.dataStructure.{KeyMapping, PatternInstance}
import org.apache.spark.Logo.Physical.utlis.TestUtil
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
    val lKeyMapping = KeyMapping(0,1,2)
    val rKeyMapping = KeyMapping(3)

    val pattern1 = PatternInstance(list1)
    val pattern2 = PatternInstance(list2)

    val pattern3 = PatternInstance.build(pattern1,lKeyMapping,pattern2,rKeyMapping)

    assert(TestUtil.listEqual(PatternInstance(Seq(1,2,3,4)).pattern,pattern3.pattern),"build failed")


  }

}
