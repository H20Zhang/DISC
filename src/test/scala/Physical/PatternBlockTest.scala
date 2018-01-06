package Physical

import TestData.TestPatternBlockData
import org.apache.spark.Logo.Physical.dataStructure._
import org.apache.spark.Logo.Physical.utlis.TestUtil
import org.scalatest.FunSuite

class PatternBlockTest extends FunSuite{

  test("ConcretePatternBlock"){
    val edgeBlock = TestPatternBlockData.edgeBlock
    println(edgeBlock)
  }

  test("KeyValueBlock"){
    val edgeBlock = TestPatternBlockData.edgeBlock.toKeyValueLogoBlock(Set(0))
    val value = edgeBlock.getValue(KeyPatternInstance(Seq(1)))
    assert(
      TestUtil.listEqual(value.get,
        Seq.fill(10)(ValuePatternInstance(Seq(2)))))
  }

  test("PlannedTwoCompositeBlock"){
    val planned2Schema = LogoSchema(KeyMapping(Seq(3,3,3)))
    val oldSchema = TestPatternBlockData.edgeBlock.schema
    val keyValuesSchema = TestPatternBlockData.keyValueEdgeBlock.schema
    val keyMappings = Seq(
      KeyMapping(Map((0,1),(1,2))),
      KeyMapping(Map((0,1),(1,0)))
    )

    val subSchemas = Seq(oldSchema,keyValuesSchema)
    val planned2CompositeSchema = PlannedTwoCompositeLogoSchema(
      0,
      planned2Schema,
      subSchemas,
      keyMappings
    )

    val metaData = LogoMetaData(Seq(2,1,2),10)
    val subBlocks = Seq(TestPatternBlockData.edgeBlock,TestPatternBlockData.keyValueEdgeBlock)


    val compositeLogoBlock = new CompositeTwoPatternLogoBlock(planned2CompositeSchema,metaData, subBlocks)

    assert(
      TestUtil.listEqual(compositeLogoBlock.assemble(),
        Seq.fill(100)(ValuePatternInstance(Seq(2,1,2)))))


  }

}
