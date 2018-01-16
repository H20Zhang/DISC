package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.FilteringCondition
import org.apache.spark.Logo.UnderLying.TestData.TestLogoRDDReferenceData
import org.apache.spark.Logo.UnderLying.dataStructure.{ConcretePatternLogoBlock, KeyMapping}

class ExamplePattern(data:String) {

  lazy val edge = {

    //    "./wikiV.txt"
    //  val dataSource="./debugData.txt"
    //  val dataSource = "/Users/zhanghao/Downloads/as-skitter.txt"

    new EdgeLoader(data,Seq(8,8)) edgeLogoRDDReference
  }

  //simple pattern
  lazy val triangle = {
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    },true)

    val filteredEdge = edge
      .filter(filterCondition)

    val leftEdge = filteredEdge.toIdentitySubPattern()
    val rightEdge = filteredEdge.toSubPattern((0,1),(1,2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
    val middleEdge = filteredEdge.toSubPattern((0,0),(1,2))

    val triangle = wedge.build(middleEdge)
    triangle
  }

  lazy val fourClique = {

  }



  lazy val threeLine = {

  }

  lazy val square = {

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    },true)

    val filteredEdge = edge
      .filter(filterCondition)

          val leftEdge = filteredEdge.toIdentitySubPattern()
          val rightEdge = filteredEdge.toSubPattern((0,0),(1,2))


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(2)
    },false)

    val wedge = leftEdge.build(rightEdge).filter(filterCondition1).toIdentitySubPattern()
          val middleEdge = edge.toSubPattern((0,2),(1,3))


    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    },false)


          val threeLine = wedge.build(middleEdge).toIdentitySubPattern()
          val lastEdge = edge.toSubPattern((0,1),(1,3))

          val square = threeLine.build(lastEdge).filter(filterCondition2)
          square
  }

  lazy val chordalSquareFast = {

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    },true)

        val edge3_1 =  new EdgeLoader(data,Seq(8,1)) edgeLogoRDDReference
        val edge3_3 =  new EdgeLoader(data,Seq(8,8)) edgeLogoRDDReference
        val leftEdge = edge3_3.filter(filterCondition1).toIdentitySubPattern()
        val rightEdge = edge3_1.toSubPattern((0,1),(1,2))

        val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()


        val middleEdge = edge3_1.toSubPattern((0,0),(1,2))

        val triangle = wedge.build(middleEdge).toConcrete()

        val leftTriangle = triangle.toIdentitySubPattern()
        val rightTriangle = triangle.toSubPattern((0,0),(1,1),(2,3))

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(2) < pattern.pattern(3)
    },false)

        val chordalSquare = leftTriangle.build(rightTriangle).filter(filterCondition2)

        chordalSquare
  }

  lazy val chordalSquare = {
    val edge = this.edge
    val leftEdge = edge.toSubPattern((0,0),(1,1))
    val rightEdge = edge.toSubPattern((0,1),(1,2))


    val wedge = leftEdge.build(rightEdge).toSubPattern((0,0),(1,1),(2,2))

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    },true)

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0,0),(1,2))

    val triangle = wedge.build(middleEdge).toConcrete()

    val leftTriangle = triangle.toIdentitySubPattern()
    val rightTriangle = triangle.toSubPattern((0,0),(2,2),(1,3))

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(3)
    },false)

    val chordalSquare = leftTriangle.build(rightTriangle).filter(filterCondition2)

    chordalSquare
  }


  //TODO:testing this
  lazy val house = {

    val edge = this.edge

    val leftEdge = edge.toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0,1),(1,2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
    val middleEdge = edge.toSubPattern((0,2),(1,3))

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    },false)

    val threeLine = wedge.build(middleEdge).filter(filterCondition)

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(2)
    },false)

    val middleEdge2 = edge.toSubPattern((0,0),(1,2))
    val triangle = wedge.build(middleEdge2).filter(filterCondition1)

    threeLine.toIdentitySubPattern().build(triangle.toSubPattern((0,0),(2,3),(1,4)))
  }

  lazy val threeTriangle = {
        val edge = this.edge
        val leftEdge = edge.toIdentitySubPattern()
        val rightEdge = edge.toSubPattern((0,0),(1,2))

        val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
        val middleEdge = edge.toSubPattern((0,1),(1,2))


        val triangle = wedge.build(middleEdge).toConcrete()

        val leftTriangle = triangle.toIdentitySubPattern()
        val rightTriangle = triangle.toSubPattern((0,0),(2,2),(1,3))


    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(2) < pattern.pattern(3)
    },false)

        val chordalSquare = leftTriangle.build(rightTriangle).filter(filterCondition).toIdentitySubPattern()


        val rightRightTriangle = triangle.toSubPattern((0,0),(1,3),(2,4))


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) != pattern.pattern(4) && pattern.pattern(2) != pattern.pattern(4) && pattern.pattern(1) != pattern.pattern(3)
    },false)

        val threeTriangle = chordalSquare.build(rightRightTriangle).filter(filterCondition1)

        threeTriangle
  }

  lazy val near5Clique = {

  }

  //complex pattern
  lazy val deep2Tree = {

  }

  lazy val triangleWithlength2Path = {

  }


}
