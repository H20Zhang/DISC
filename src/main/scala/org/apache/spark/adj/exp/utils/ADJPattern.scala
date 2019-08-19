package org.apache.spark.adj.exp.utils

import org.apache.spark.adj.execution.rdd.loader.{DataLoader, EdgeLoader}

class ADJPattern(data: String, h1:Int=3, h2:Int=3)  {

//  var h1 = 13
//  var h2 = 13
  val filterCoefficient = 1

  lazy val rawEdge = {
//        new CompactEdgeLoader(data) rawEdgeRDD
    new DataLoader(data) rawEdgeRDD
  }

  lazy val edge = {

    getEdge(h1,h2)
  }

  def getEdge(hNumber: (Int, Int)) = {
//        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    rawEdge.count()
    new EdgeLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }


  def get(name:String)  ={
    name match {
      case "edge" => edge
      case "wedge" => wedge
      case "triangle" => triangle
      case "triangleEdge" => triangleEdge
      case "threePath" => threePath
      case "square" => square
      case "squareEdge" => squareEdge
      case "chordalSquare" => chordalSquare
      case "fourClique" => fourClique
      case "house" => house
      case "threeTriangle" => threeTriangle
      case "threeCenterTriangle" => threeCenterTriangle
      case "near5Clique" => near5Clique
      case "twoHeadTriangle" => twoHeadTriangle
      case "fourCliqueEdge" => fourCliqueEdge
      case _ => null
    }
  }


  lazy val wedge = {
    val edge = getEdge(h1,h2)
    edge.build(edge.to(0,2))
  }


  lazy val triangle = {
    val edge = getEdge(h1,h2)
    val triangle =  edge.build(edge.to(1,2),edge.to(0,2))
    triangle
  }

  lazy val triangleEdge = {
    val edge = getEdge(h1,h2)

    val triangle =  edge.build(edge.to(1,2),edge.to(0,2))

    val triangleEdge = triangle.build(edge.to(0,3))

    triangleEdge
  }

  lazy val threePath = {
    val edge = getEdge(h1,h2)
    val wedge = edge.build(edge.to(0,2))
    val threePath = wedge.build(edge.to(1, 3))

    threePath
  }

  lazy val square = {
    val edge4_1 = getEdge(h1, h2)
    val edge4_4 = getEdge(h1, h2)

    val wedge = edge4_4
      .build(edge4_4.to(0,2))

    val square = wedge.build(edge4_1.to(1,3), edge4_1.to(2,3))
    square
  }

  lazy val squareEdge = {
    val edge = getEdge(h1, h2)

    val wedge = edge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))

    val squareEdge = square.build(edge.to(0,4))

    squareEdge
  }

  lazy val chordalSquare = {

    val edge3_1 = getEdge(h1, 1)
    val edge3_3 = getEdge(h1, h2)

    val triangle = edge3_3.build(edge3_1.to(1,2), edge3_1.to(0,2))

    val chordalSquare = triangle.build(triangle.to(0,1,3))

    chordalSquare
  }

  lazy val fourClique = {
    val filteredEdge = getEdge(h1, h2)
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))

    val filteredEdge4_1 = getEdge(h1,h2)

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3))
    fourClique
  }

  lazy val house = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val leftEdge = edge4_4.filter(p => p(0) < p(1))
    val wedge = leftEdge.build(edge4_4.to(1,2))

    val squareTemp = wedge.build(edge4_1.to(2,3), edge4_1.to(0,3))
      .filter(p => p(0) != p(2) && p(1) != p(3))

    val indexTriangle = leftEdge.build(edge4_1.to(1,2), edge4_1.to(0,2))

    val house = squareTemp.build(indexTriangle.to(0,1,4))
      .filter(p => p(3) != p(4) && p(2) != p(4))

    house
  }

  lazy val threeCenterTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(edge4_1.to(0,3),edge4_1.to(1,3))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,1,4))

    threeTriangle

  }

  lazy val twoHeadTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))
      .filter(p => p(0) < p(2))
//

    val trianglePlusOneEdge= triangle.build(edge4_4.to(1,3))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.filter(p => p(0) < p(1)).to(1,2))

    val twoHeadTriangle = trianglePlusOneEdge.build(indexTriangle.to(1,3,4))

    twoHeadTriangle
  }

  lazy val threeTriangle = {
    val edge4_1 = getEdge(h1, 1)
    val edge4_4 = getEdge(h1, h2)

    val triangle = edge4_4.build(edge4_4.to(0,2), edge4_4.to(1,2))
//      .filter(p => p(1) < p(2))

    val indexTriangle = edge4_4.build(edge4_1.to(0,2), edge4_1.to(1,2))

    val chordalSquareTemp = triangle.build(edge4_1.to(0,3),edge4_1.to(1,3))

    val threeTriangle = chordalSquareTemp.build(indexTriangle.to(0,2,4))
//      .filter(p => p(3) != p(2) && p(4) != p(1) && p(3) != p(4))

    threeTriangle
  }


  lazy val fourCliqueEdge = {
    val edge4_1 = getEdge(h1,1)

    val triangle =  edge.build(edge.to(1,2),edge.to(0,2))
    
    val fourClique = triangle.build(edge4_1.to(0,3),edge4_1.to(1,3),edge4_1.to(2,3))

    val fourCliqueEdge = fourClique.build(edge4_1.to(0,4))


    fourCliqueEdge
  }


  lazy val near5Clique = {
    val filteredEdge = edge
    val triangle =  filteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))
//      .filter(p => p(0) < p(1))

    val filteredEdge4_1 = getEdge(h1,1)

    val fourClique = triangle.build(filteredEdge4_1.to(0,3),filteredEdge4_1.to(1,3),filteredEdge4_1.to(2,3))

    val indexTriangle = filteredEdge.build(filteredEdge4_1.to(1,2),filteredEdge4_1.to(0,2))
//      .filter(p => p(0) < p(1))

    val near5Clique = fourClique.build(indexTriangle.to(0,1,4))
//      .filter(p => p(4) != p(2) && p(4) != p(3))


    near5Clique
  }

}
