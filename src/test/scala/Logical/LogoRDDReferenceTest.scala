package Logical

import TestData.TestLogoRDDReferenceData
import org.apache.spark.Logo.Physical.dataStructure.{CompositeLogoSchema, KeyMapping}
import org.scalatest.FunSuite

class LogoRDDReferenceTest extends FunSuite{

  test("buildLogicalTest"){


    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
    val leftEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,1))))
    val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
    val wedge = leftEdge.build(rightEdge)

    println(wedge.patternSchema.asInstanceOf[CompositeLogoSchema].keyMappings)
  }
}
