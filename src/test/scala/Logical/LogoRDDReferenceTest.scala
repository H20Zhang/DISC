package Logical

import TestData.TestLogoRDDReferenceData
import org.apache.spark.Logo.Physical.dataStructure.{CompositeLogoSchema, CompositeTwoPatternLogoBlock, ConcretePatternLogoBlock, KeyMapping}
import org.scalatest.FunSuite

class LogoRDDReferenceTest extends FunSuite{

//  test("wedgeTest"){
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//    val leftEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,1))))
//    val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//    val wedge = leftEdge.build(rightEdge)
//
////    edgeRDDReference.generateJ().logoRDD.map{
////      f =>
////      val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
////      if (concreteBlock.rawData.toList.size > 0){
////        println("")
////        println(concreteBlock.metaData)
////        println(concreteBlock.schema)
////        println(concreteBlock.rawData.toList.size)
////      }
////    }.count()
////
//    wedge.generateF().logoRDD.map{
//      f =>
//
//        val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
//        println("")
//        println(compositeBlock.metaData)
//        println(compositeBlock.schema)
////        println(compositeBlock.leafsBlock.rawData)
////        println(compositeBlock.coreBlock.rawData)
//
//
//        val leafRawData = compositeBlock.leafsBlock.rawData
//        val coreRawData = compositeBlock.coreBlock.rawData
//
//
//
//
//
//    }.count()
//
//
//    wedge.generateJ().logoRDD.map{
//      f =>
//        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
////        if (concreteBlock.rawData.toList.size > 0){
//          println("")
//          println(concreteBlock.metaData)
//          println(concreteBlock.schema)
//          println(concreteBlock.rawData.toList.size)
////        }
//    }.count()
//
//    println(wedge.patternSchema.asInstanceOf[CompositeLogoSchema].keyMappings)
//  }

//  test("triangleTest"){
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//    val leftEdge = edgeRDDReference.toIdentitySubPattern()
//    val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//
//    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
//    val middleEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,2))))
//
//    val triangle = wedge.build(middleEdge)
//
//    val triangleCount = triangle.generateF().logoRDD.map{
//      f =>
//
//        val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
//        val size = compositeBlock.enumerateIterator().size
//        println(compositeBlock.leafsBlock.schema)
//        println(compositeBlock.coreBlock.schema)
//        println(size)
//        size
//    }.sum()
//
////    val triangleCount = triangle.generateJ().logoRDD.map{
////      f =>
////        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
////        if (concreteBlock.rawData.toList.size > 0){
////          println("")
////          println(concreteBlock.metaData)
////          println(concreteBlock.schema)
////          println(concreteBlock.rawData.toList.size)
////        }
////        concreteBlock.rawData.toList.size
////
////    }.sum()
//
//    println(s"triangle number is $triangleCount")
//  }

//  test("chordalSquareTest"){
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//    val leftEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,1))))
//    val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//
//    val wedge = leftEdge.build(rightEdge).toSubPattern(KeyMapping(Map((0,0),(1,1),(2,2))))
//    val middleEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,2))))
//
//    val triangle = wedge.build(middleEdge)
//
//    val leftTriangle = triangle.toSubPattern(KeyMapping(Map((0,0),(1,1),(2,2))))
//    val rightTriangle = triangle.toSubPattern(KeyMapping(Map((0,3),(1,1),(2,2))))
//
//    val chordalSquare = leftTriangle.build(rightTriangle)
//
//    val squareCount = chordalSquare.generateF().logoRDD.map{
//      f =>
//
//        val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
//        println(compositeBlock.schema)
//        println(compositeBlock.leafsBlock.schema)
//        println(compositeBlock.coreBlock.schema)
//        val size = compositeBlock.enumerateIterator().size
////        val size = compositeBlock.iterator().size
//        println(size)
//        size
//    }.sum()
//
//
//    println(s"chordalsquare number is $squareCount")
//  }
//
  test("threeTriangleTest"){
    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
    val leftEdge = edgeRDDReference.toIdentitySubPattern()
    val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
    val middleEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,2))))

    val triangle = wedge.build(middleEdge)

    val leftTriangle = triangle.toIdentitySubPattern()
    val rightTriangle = triangle.toSubPattern(KeyMapping(Map((0,3),(1,1),(2,2))))

    val chordalSquare = leftTriangle.build(rightTriangle).toIdentitySubPattern()
    val rightRightTriangle = triangle.toSubPattern(KeyMapping(Map((0,0),(1,3),(2,4))))
    val threeTriangle = chordalSquare.build(rightRightTriangle)

    val threeTriangleCount = threeTriangle.generateF().logoRDD.map{
      f =>

        val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
        println(compositeBlock.schema)
        println(compositeBlock.leafsBlock.schema)
        println(compositeBlock.coreBlock.schema)
        val size = compositeBlock.enumerateIterator().size
        //        val size = compositeBlock.iterator().size
        println(size)
        size
    }.sum()


    println(s"three Triangle number is $threeTriangleCount")
  }

}
