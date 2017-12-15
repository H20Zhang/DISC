import TestData.TestPatternBlockData
import org.apache.spark.Logo.Physical.dataStructure.{KeyPatternInstance, PlannedTwoCompositeLogoSchema, ValuePatternInstance}
import org.apache.spark.Logo.Physical.utlis.TestUtil
import org.scalatest.FunSuite

class PatternBlockTest extends FunSuite{

  test("ConcretePatternBlock"){
    val edgeBlock = TestPatternBlockData.edgeBlock
    println(edgeBlock)
  }

  test("KeyValueBlock"){
    val edgeBlock = TestPatternBlockData.edgeBlock.toKeyValueLogoBlock(Seq(0))
    val value = edgeBlock.getValue(KeyPatternInstance(Seq(1)))
    assert(
      TestUtil.listEqual(value,
        Seq.fill(10)(ValuePatternInstance(Seq(2)))))
  }

  test("PlannedTwoCompositeBlock"){
    val planned2Schema = ???
    val planned2CompositeSchema = ???

  }

}
