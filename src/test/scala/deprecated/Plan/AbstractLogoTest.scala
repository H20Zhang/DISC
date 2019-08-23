package deprecated.Plan

import org.scalatest.FunSuite

class AbstractLogoTest extends FunSuite{

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


//  test("filtering"){
//
//    val filterCondition = FilteringCondition({
//            pattern =>
//              pattern.pattern(0) < pattern.pattern(1)
//          },true)
//
//    val filteredEdgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//      .filter(filterCondition)
//
//    filteredEdgeRDDReference.generateF().toKeyValuePatternLogoRDD(Set(0))
//    filteredEdgeRDDReference.generateJ()
//
//  }


//  test("filterCondition-Triangle"){
//
//    val filterCondition = FilteringCondition({
//      pattern =>
//        pattern.pattern(0) < pattern.pattern(1)
//    },true)
//
//
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//
//    edgeRDDReference.generateJ().patternRDD.map{
//      f =>
//        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
//        //                    if (concreteBlock.rawData.toList.size > 0){
//        println("")
//        println(concreteBlock.metaData)
//        println(concreteBlock.schema)
//        println(concreteBlock.rawData.toList.size)
//        1
//      //                    }else{
//      //                      0
//      //                    }
//    }.count()
//        val filteredEdgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//          .filter(filterCondition)
//
//
////        val filteredBuildScript = new LogoFilterPatternPhysicalPlan(filterCondition,edgeRDDReference.buildScript)
////         val newBuildScript = new LogoEdgePatternPhysicalPlan(filteredBuildScript.generateNewPatternJState())
////         val filteredEdgeRDDReference =  new PatternLogoRDDReference(edgeRDDReference.patternSchema,newBuildScript)
////
////
////
////    filteredEdgeRDDReference.generateJ().patternRDD.map{
////      f =>
////        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
//////                    if (concreteBlock.rawData.toList.size > 0){
////                      println("")
////                      println(concreteBlock.metaData)
////                      println(concreteBlock.schema)
////                      println(concreteBlock.rawData.toList.size)
////                      1
//////                    }else{
//////                      0
//////                    }
////    }.count()
////
//        val leftEdge = filteredEdgeRDDReference.toIdentitySubPattern()
//        val rightEdge = filteredEdgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//
////        val wedge = leftEdge.build(rightEdge).generateF().logoRDD.map{
////          f=>
////          val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
////                      val size = compositeBlock.enumerateIterator().size
////                      println(compositeBlock.leafsBlock.schema)
////                      println(compositeBlock.coreBlock.schema)
////                      println(size)
////                      size
////        }
//
//        val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
//
//        val middleEdge = filteredEdgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,2))))
//
//        val triangle = wedge.build(middleEdge)
//
//        val triangleCount = triangle.size()
//
////          generateF().logoRDD.map{
////          f =>
////
////            val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
////            val size = compositeBlock.enumerateIterator().size
////            println(compositeBlock.leafsBlock.schema)
////            println(compositeBlock.coreBlock.schema)
////            println(size)
////            size
////        }.sum()
//
//        assert(triangleCount == 608389)
//  }

//  test("filterCondition-notStrict"){
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//
//    val filterCondition = FilteringCondition({
//      pattern =>
//        pattern == PatternInstance(Seq(7115,201524))
//    },false)
//
//
//    //for
//    val filteredEdgeCount = edgeRDDReference.filter(filterCondition).generateF().logoRDD.map{
//      f =>
//        val filteringBlock = f.asInstanceOf[FilteringPatternLogoBlock[_]]
//        val concreteBlock = filteringBlock.rawData.asInstanceOf[Seq[PatternInstance]]
//
//          println("")
//          println(filteringBlock.metaData)
//          println(filteringBlock.schema)
//          println(filteringBlock.iterator().size)
//          println(concreteBlock.toList.size)
//         concreteBlock.toList.size
//    }.sum()
//
//    assert(filteredEdgeCount == 201526.0)
//  }
//
//  test("filterCondition-set-unset"){
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//
//    val filterCondition = FilteringCondition({
//      pattern =>
//        pattern == PatternInstance(Seq(7115,201524))
//    },true)
//
//
//
//    val filteredEdgeCount = edgeRDDReference.filter(filterCondition).generateJ().logoRDD.map{
//      f =>
//      val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
//              if (concreteBlock.rawData.toList.size > 0){
//                println("")
//                println(concreteBlock.metaData)
//                println(concreteBlock.schema)
//                println(concreteBlock.rawData.toList.size)
//                1
//              }else{
//                0
//              }
//    }.sum()
//
//    assert(filteredEdgeCount == 1)
//
//
//    val edgeCount = edgeRDDReference.generateJ().logoRDD.map{
//      f =>
//        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
//
//        println("")
//        println(concreteBlock.metaData)
//        println(concreteBlock.schema)
//        println(concreteBlock.rawData.toList.size)
//        concreteBlock.rawData.toList.size
//
//    }.sum()
//
//    assert(edgeCount == 201526.0)
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
////   triangle.generateJ().logoRDD.map{
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

//    test("square-WedgeTest"){
//      val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//      val leftEdge = edgeRDDReference.toIdentitySubPattern()
//      val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//
//      val wedge = leftEdge.build(rightEdge)
//
//
//      val leftWedge = wedge.toIdentitySubPattern()
//
//      val rightFilteringCondition = FilteringCondition(
//        {
//          p =>
//            p.pattern(1) < p.pattern(0) && p.pattern(1) < p.pattern(2) && p.pattern(0) < p.pattern(2)
//        },true
//      )
//      val rightWedge = wedge.filter(rightFilteringCondition).toSubPattern(KeyMapping(Map((0,0),(1,3),(2,2))))
//
//
//      val square = leftEdge.build(rightWedge)
//
//      val squareFilteringCondition = FilteringCondition(
//        {
//          p =>
//            p.pattern(1) < p.pattern(3)
//        },false
//      )
//
//
//      val triangleCount = square
//        .filter(squareFilteringCondition)
//        .generateF().logoRDD.map{
//        f =>
//
//          val compositeBlock = f.asInstanceOf[PatternLogoBlock[_]]
//          val size = compositeBlock.enumerateIterator().size
////          println(compositeBlock.leafsBlock.schema)
////          println(compositeBlock.coreBlock.schema)
//          println(size)
//          size
//      }.sum()
//
//  //   triangle.generateJ().logoRDD.map{
//  //      f =>
//  //        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
//  //        if (concreteBlock.rawData.toList.size > 0){
//  //          println("")
//  //          println(concreteBlock.metaData)
//  //          println(concreteBlock.schema)
//  //          println(concreteBlock.rawData.toList.size)
//  //        }
//  //        concreteBlock.rawData.toList.size
//  //
//  //    }.sum()
//
//      println(s"triangle number is $triangleCount")
//    }


//    test("squareTest"){
//      val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//      val leftEdge = edgeRDDReference.toIdentitySubPattern()
//      val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//
//      val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
//      val middleEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,2),(1,3))))
//
//      val threeLine = wedge.build(middleEdge).toIdentitySubPattern()
//      val lastEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,3))))
//
//      val square = threeLine.build(lastEdge)
//
//
//      val triangleCount = square.generateF().logoRDD.map{
//        f =>
//
//          val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
//          val size = compositeBlock.enumerateIterator().size
//          println(compositeBlock.leafsBlock.schema)
//          println(compositeBlock.coreBlock.schema)
//          println(size)
//          size
//      }.sum()
//
//  //   triangle.generateJ().logoRDD.map{
//  //      f =>
//  //        val concreteBlock = f.asInstanceOf[ConcretePatternLogoBlock]
//  //        if (concreteBlock.rawData.toList.size > 0){
//  //          println("")
//  //          println(concreteBlock.metaData)
//  //          println(concreteBlock.schema)
//  //          println(concreteBlock.rawData.toList.size)
//  //        }
//  //        concreteBlock.rawData.toList.size
//  //
//  //    }.sum()
//
//      println(s"triangle number is $triangleCount")
//    }


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
//  test("threeTriangleTest"){
//    val edgeRDDReference = TestLogoRDDReferenceData.edgeLogoRDDReference
//    val leftEdge = edgeRDDReference.toIdentitySubPattern()
//    val rightEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,1),(1,2))))
//
//    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
//    val middleEdge = edgeRDDReference.toSubPattern(KeyMapping(Map((0,0),(1,2))))
//
//    val triangle = wedge.build(middleEdge)
//
//    val leftTriangle = triangle.toIdentitySubPattern()
//    val rightTriangle = triangle.toSubPattern(KeyMapping(Map((0,3),(1,1),(2,2))))
//
//    val chordalSquare = leftTriangle.build(rightTriangle).toIdentitySubPattern()
//    val rightRightTriangle = triangle.toSubPattern(KeyMapping(Map((0,0),(1,3),(2,4))))
//    val threeTriangle = chordalSquare.build(rightRightTriangle)
//
//    val threeTriangleCount = threeTriangle.generateF().logoRDD.map{
//      f =>
//
//        val compositeBlock = f.asInstanceOf[CompositeTwoPatternLogoBlock]
//        println(compositeBlock.schema)
//        println(compositeBlock.leafsBlock.schema)
//        println(compositeBlock.coreBlock.schema)
//        val size = compositeBlock.enumerateIterator().size
//        //        val size = compositeBlock.iterator().size
//        println(size)
//        size
//    }.sum()
//
//
//    println(s"three Triangle number is $threeTriangleCount")
//  }

}
