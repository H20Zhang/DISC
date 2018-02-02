package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.FilteringCondition

class ExamplePattern(data: String) {

  var h1 = 6
  var h2 = 6

  lazy val rawEdge = {
//        new CompactEdgeLoader(data) rawEdgeRDD
    new EdgeLoader(data) rawEdgeRDD
  }

  lazy val edge = {
    getEdge(h1,h2)
  }

  def getEdge(hNumber: (Int, Int)) = {
//        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgePatternLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }


  def pattern(name:String)  ={
    name match {
      case "triangle" => triangleIntersectionVersion
      case "chordalSquare" => chordalSquareFast
      case "square" => squareIntersectionVerificationFast
      case "house" => houseIntersectionFast
      case "houseF" => houseIntersectionF
      case "threeTriangle" => threeTriangleFast
      case "threeTriangleF" => threeTriangleF
      case "trianglePlusOneEdge" => trianglePlusOneEdge
      case "trianglePlusTwoEdgeF" => trianglePlusTwoEdgeF
      case "trianglePlusWedge" => trianglePlusWedge
      case "squarePlusOneEdgeF" => squarePlusOneEdge
    }
  }

  //simple pattern
  lazy val triangle = {
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val filteredEdge = edge
      .filter(filterCondition)

    val leftEdge = filteredEdge.toIdentitySubPattern()
    val rightEdge = filteredEdge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
    val middleEdge = filteredEdge.toSubPattern((0, 0), (1, 2))

    val triangle = wedge.build(middleEdge)
    triangle
  }


  //TODO test
  lazy val triangleIntersectionVersion = {
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.getValue(0) < pattern.getValue(1)
    }, true)

    val filteredEdge = edge
      .filter(filterCondition)

    val leftEdge = filteredEdge.toIdentitySubPattern()
    val rightEdge = filteredEdge.toSubPattern((0, 1), (1, 2))
    val middleEdge = filteredEdge.toSubPattern((0, 0), (1, 2))

    val triangle = leftEdge.build(rightEdge, middleEdge)
    triangle
  }

  //TODO finish this
  lazy val twinTriangle = {

  }


  lazy val fourClique = {

  }

  lazy val threeLine = {
    val edge = this.edge

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val leftEdge = edge.toIdentitySubPattern()
    val middleEdge = edge.filter(filterCondition).toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(middleEdge).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 2), (1, 3))

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) != pattern.pattern(3)
    }, false)

    val threeLine = wedge.build(rightEdge).filter(filterCondition1)

    threeLine
  }

  lazy val trianglePlusOneEdge = {
    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val lastEdge = getEdge(h1, h2)

    val edge = getEdge(h1, h2)
    val leftEdge = edge.filter(filterCondition1).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val triangle = wedge.build(middleEdge)

    val filterCondition2 = FilteringCondition({
      pattern =>
        val array = pattern.pattern
        array(3) != array(1) && array(3) != array(2)
    }, false)

    val triangleWithOneEdge = triangle.toIdentitySubPattern().build(lastEdge.toSubPattern((0, 0), (1, 3))).filter(filterCondition2)

    triangleWithOneEdge
  }

  lazy val trianglePlusTwoEdgeF = {

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val edge4_1 = getEdge(h1, 1)

    val edge = getEdge(h1, h2)
    val leftEdge = edge.filter(filterCondition1).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val filterCondition2 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        (p(0)%2+ p(1)%2 + p(2)%2) < 2
    },false)

    val triangle = wedge.build(middleEdge).filter(filterCondition2)

    val triangleWithOneEdge = triangle.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 0), (1, 3)))

    val filterCondition3 = FilteringCondition({
      pattern =>
        val array = pattern.pattern
        array(3) != array(1) && array(3) != array(2) && array(4) != array(0) && array(4) != array(2)
    }, false)

    val trianglePlusTwoEdge = triangleWithOneEdge.toIdentitySubPattern().build(edge4_1.toSubPattern((0, 1), (1, 4)))
      .filter(filterCondition3)

    trianglePlusTwoEdge
  }

  lazy val trianglePlusWedge = {
    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val edge = this.edge
    val leftEdge = edge.filter(filterCondition1).toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val triangle = wedge.build(middleEdge)

    val triangleWithOneEdge = triangle.toIdentitySubPattern().build(edge.toSubPattern((0, 0), (1, 3)))

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(3) != pattern.pattern(0) && pattern.pattern(3) != pattern.pattern(1) && pattern.pattern(4) != pattern.pattern(0) && pattern.pattern(4) != pattern.pattern(1) && pattern.pattern(4) != pattern.pattern(2)
    }, false)

    val triangleWithWedge = triangleWithOneEdge.toIdentitySubPattern().build(edge.toSubPattern((0, 3), (1, 4))).filter(filterCondition2)

    triangleWithWedge
  }

  lazy val squareFast = {


    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val leftEdge = edge4_4
      .filter(filterCondition)
      .toIdentitySubPattern()

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(2)
    }, false)

    val wedge = leftEdge.build(edge4_4
      .filter(filterCondition)
      .toSubPattern((0, 0), (1, 2)))
      .filter(filterCondition1)
      .toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    }, false)
    val threeLineTemp = wedge.build(edge4_1.toSubPattern((0, 1), (1, 3)))
      .filter(filterCondition2)
      .toIdentitySubPattern()
    val square = threeLineTemp.build(edge4_1.toSubPattern((0, 2), (1, 3)))
    square
  }

  lazy val squareIntersectionVerificationFast = {


    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)
    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val leftEdge = edge4_4
      .filter(filterCondition)
      .toIdentitySubPattern()

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(2)
    }, false)

    val wedge = leftEdge.build(edge4_4
      .filter(filterCondition)
      .toSubPattern((0, 0), (1, 2)))
      .filter(filterCondition1)
      .toIdentitySubPattern()


    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    }, false)
    val square = wedge.build(edge4_1.toSubPattern((0, 1), (1, 3)), edge4_1.toSubPattern((0, 2), (1, 3)))
      .filter(filterCondition2)
    //    val square = threeLineTemp.build(edge4_1.toSubPattern((0,2),(1,3)))
    square
  }


  lazy val square = {

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val filteredEdge = edge
      .filter(filterCondition)

    val leftEdge = filteredEdge.toIdentitySubPattern()
    val rightEdge = filteredEdge.toSubPattern((0, 0), (1, 2))


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(2)
    }, false)

    val wedge = leftEdge.build(rightEdge).filter(filterCondition1).toIdentitySubPattern()
    val middleEdge = edge.toSubPattern((0, 2), (1, 3))


    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    }, false)


    val threeLine = wedge.build(middleEdge).toIdentitySubPattern()
    val lastEdge = edge.toSubPattern((0, 1), (1, 3))

    val square = threeLine.build(lastEdge).filter(filterCondition2)
    square
  }

  lazy val chordalSquareFast = {

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val edge3_1 = getEdge(h1, 1)
    val edge3_3 = getEdge(h1, h2)
    val leftEdge = edge3_3.filter(filterCondition1).toIdentitySubPattern()

    val triangle = leftEdge.build(edge3_1.toSubPattern((0, 1), (1, 2)), edge3_1.toSubPattern((0, 0), (1, 2)))


    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(2) < pattern.pattern(3)
    }, false)

    val chordalSquare = triangle
      .toIdentitySubPattern()
      .build(triangle.toSubPattern((0, 0), (1, 1), (2, 3)))
      .filter(filterCondition2)

    chordalSquare
  }

  lazy val chordalSquare = {
    val edge = this.edge
    val leftEdge = edge.toSubPattern((0, 0), (1, 1))
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))


    val wedge = leftEdge.build(rightEdge).toSubPattern((0, 0), (1, 1), (2, 2))

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, true)

    val middleEdge = edge.filter(filterCondition1).toSubPattern((0, 0), (1, 2))

    val triangle = wedge.build(middleEdge).toConcrete()

    val leftTriangle = triangle.toIdentitySubPattern()
    val rightTriangle = triangle.toSubPattern((0, 0), (2, 2), (1, 3))

    val filterCondition2 = FilteringCondition({
      pattern =>
        pattern.pattern(1) < pattern.pattern(3)
    }, false)

    val chordalSquare = leftTriangle.build(rightTriangle).filter(filterCondition2)

    chordalSquare
  }

  lazy val squarePlusOneEdge = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_2 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(1) < p(3) && (p(0)%2+ p(1)%2 + p(2)%2 + p(3) %2) < 2
    }, false)


    val leftEdge = edge4_4.toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()



    val squareTemp = wedge.build(edge4_2.toSubPattern((0, 2), (1, 3)), edge4_2.toSubPattern((0, 0), (1, 3))).filter(filterCondition).toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(0) != p(2) && p(4) != p(1) && p(4) != p(2) && p(4) != p(3)
    }, false)

    val squarePlusOneEdge = squareTemp.build(edge4_1.toSubPattern((0, 0), (1, 4))).filter(filterCondition2)

    squarePlusOneEdge
  }


  lazy val houseFast = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(1)
    }, false)

    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()
    val threeLine = wedge.build(edge4_1.toSubPattern((0, 2), (1, 3))).toIdentitySubPattern()
    val squareTemp = threeLine.build(edge4_1.toSubPattern((0, 0), (1, 3))).toIdentitySubPattern()


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) != pattern.pattern(2) && pattern.pattern(1) != pattern.pattern(3) && pattern.pattern(3) != pattern.pattern(4) && pattern.pattern(2) != pattern.pattern(4)
    }, false)

    val triangleLeftEdge = leftEdge
    val triangleWedge = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()
    val indexTriangle = triangleWedge.build(edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4))).filter(filterCondition1)

    house
  }

  lazy val houseIntersectionFast = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_2 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({ p =>
      p.pattern(0) < p.pattern(1)
    }, true)


    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()

    val squareTemp = wedge.build(edge4_2.toSubPattern((0, 2), (1, 3)), edge4_2.toSubPattern((0, 0), (1, 3))).toIdentitySubPattern()


    val filterCondition2 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(3) != p(4) && p(2) != p(4) && p(0) != p(2) && p(1) != p(3)
    }
      , false)


    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2)))

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(filterCondition2)

    house
  }

  lazy val houseIntersectionF = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_2 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val filterCondition = FilteringCondition({ p =>
      p.pattern(0) < p.pattern(1)
    }, true)

    val leftEdge = edge4_4.filter(filterCondition).toIdentitySubPattern()
    val wedge = leftEdge.build(edge4_4.toSubPattern((0, 1), (1, 2))).toIdentitySubPattern()

    val filterCondition1 = FilteringCondition({
      pattern =>
            val p = pattern.pattern
        (((p(0)*31+p(1))*31+p(2))*31+p(3)) % 10 < 5
        },false)

    val squareTemp = wedge.build(edge4_2.toSubPattern((0, 2), (1, 3)), edge4_2.toSubPattern((0, 0), (1, 3))).filter(filterCondition1)
      .toIdentitySubPattern()

    val filterCondition2 = FilteringCondition({
        pattern =>
          val p = pattern.pattern
          ((p(0)*31+p(1))*31+p(2)) % 10 < 5
      },false)

    val filterCondition3 = FilteringCondition({ pattern =>
      val p = pattern.pattern
      p(3) != p(4) && p(2) != p(4) && p(0) != p(2) && p(1) != p(3)
    }, false)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 1), (1, 2)), edge4_1.toSubPattern((0, 0), (1, 2))).filter(filterCondition2)

    val house = squareTemp.build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 4)))
      .filter(filterCondition3)

    house
  }

  //TODO:testing this
  lazy val house = {


    val edge = this.edge

    val leftEdge = edge.toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 1), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
    val middleEdge = edge.toSubPattern((0, 2), (1, 3))

    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(3)
    }, false)

    val threeLine = wedge.build(middleEdge).filter(filterCondition)

    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(0) < pattern.pattern(2)
    }, false)

    val middleEdge2 = edge.toSubPattern((0, 0), (1, 2))
    val triangle = wedge.build(middleEdge2).filter(filterCondition1)

    threeLine.toIdentitySubPattern().build(triangle.toSubPattern((0, 0), (2, 3), (1, 4)))
  }

  lazy val threeTriangleF = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        ((p(0)*31+p(1))*31+p(2)) % 10 < 5 && p(1) < p(2)
    }, false)

//    ((p(0)*31+p(1))*31+p(2) % 10) < 2
    //    (p(0)%2+ p(1)%2 + p(2)%2) < 2

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

        val triangleFilterCondition = FilteringCondition({
          pattern =>
            val p = pattern.pattern
            ((p(0)*31+p(1))*31+p(2)) % 10 < 5
//              (p(0)%2+ p(1)%2 + p(2)%2) < 2
        },false)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2))).filter(triangleFilterCondition)

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 2), (2, 4)))
      .filter(filterCondition1)

    threeTriangle
  }


  lazy val threeTriangleFast = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.toIdentitySubPattern()

    val filterCondition = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(1) < p(2)
    }, false)

    val triangle = leftEdge.build(edge4_4.toSubPattern((0, 0), (1, 2)), edge4_4.toSubPattern((0, 1), (1, 2))).filter(filterCondition)

    val indexTriangle = leftEdge.build(edge4_1.toSubPattern((0, 0), (1, 2)), edge4_1.toSubPattern((0, 1), (1, 2)))

    val chordalSquareTemp = triangle.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 1), (2, 3)))

    val filterCondition1 = FilteringCondition({
      pattern =>
        val p = pattern.pattern
        p(3) != p(2) && p(4) != p(1) && p(3) != p(4)
    }, false)

    val threeTriangle = chordalSquareTemp.toIdentitySubPattern().build(indexTriangle.toSubPattern((0, 0), (1, 2), (2, 4)))
      .filter(filterCondition1)

    threeTriangle
  }


  lazy val threeTriangle = {
    val edge = this.edge

    val leftEdge = edge.toIdentitySubPattern()
    val rightEdge = edge.toSubPattern((0, 0), (1, 2))

    val wedge = leftEdge.build(rightEdge).toIdentitySubPattern()
    val middleEdge = edge.toSubPattern((0, 1), (1, 2))


    val triangle = wedge.build(middleEdge).toConcrete()

    val leftTriangle = triangle.toIdentitySubPattern()
    val rightTriangle = triangle.toSubPattern((0, 0), (2, 2), (1, 3))


    val filterCondition = FilteringCondition({
      pattern =>
        pattern.pattern(2) < pattern.pattern(3)
    }, false)

    val chordalSquare = leftTriangle.build(rightTriangle).filter(filterCondition).toIdentitySubPattern()


    val rightRightTriangle = triangle.toSubPattern((0, 0), (1, 3), (2, 4))


    val filterCondition1 = FilteringCondition({
      pattern =>
        pattern.pattern(1) != pattern.pattern(4) && pattern.pattern(2) != pattern.pattern(4) && pattern.pattern(1) != pattern.pattern(3)
    }, false)

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
