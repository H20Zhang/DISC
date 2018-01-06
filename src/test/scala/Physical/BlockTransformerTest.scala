package Physical

import TestData.TestLogoRDDReferenceData
import org.apache.spark.Logo.Physical.dataStructure.{ConcretePatternLogoBlock, KeyValuePatternLogoBlock}
import org.scalatest.FunSuite

class BlockTransformerTest extends FunSuite{

  test("ToKeyValueTransformer"){
    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
    val edgeRDD = edgeRDDReference.generateF()

    val keyValueRDD = edgeRDD.toKeyValuePatternLogoRDD(Set(0))

    keyValueRDD.patternRDD.foreach { f =>

//      val keyValueBlock = f.asInstanceOf[KeyValuePatternLogoBlock]
//      println(keyValueBlock.rawData)
    }

    keyValueRDD.patternRDD.count()

    println(keyValueRDD.patternSchema)
  }

  test("ToConcreteTransformer"){
    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
    val edgeRDD = edgeRDDReference.generateF()

    val keyValueRDD = edgeRDD.toConcretePatternLogoRDD

    keyValueRDD.patternRDD.foreach{
      f =>
        val block = f.asInstanceOf[ConcretePatternLogoBlock]
        println(block.rawData)
    }

    println(keyValueRDD.patternSchema)
  }



}
