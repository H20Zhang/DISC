package org.apache.spark.adj.exp.utils



class ADJPatternStatistic(data: String, h1:Int=3, h2:Int=3)  {

  private val pattern = new ADJPattern(data, h1, h2)

  def get(name:String) = name match {
    case "house" => houseStatistic
    case "near5Clique" => near5CliqueStatistic
  }



  lazy val houseStatistic = {
//    Edge
    val edgeSize = 0

//    wedge
    val wedgeSize = pattern.get("wedge").size()

//    threePath
    val threePathSize = pattern.get("threePath").size()

//    square
    val squareSize = pattern.get("square").size()

//    squareEdge
    val squareEdgeSize = pattern.get("squareEdge").size()

//    total size
    val size =
          edgeSize + edgeSize +
          edgeSize + wedgeSize +
          edgeSize + threePathSize +
          edgeSize + squareSize +
          edgeSize + squareEdgeSize

    size
  }

  lazy val near5CliqueStatistic = {
    //    Edge
    val edgeSize = 0

    //    triangle
    val triangleSize = pattern.get("triangle").size()

    //    triangleEdge
    val triangleEdgeSize = pattern.get("triangleEdge").size()

    //    chordalSquare
    val chordalSquareSize = pattern.get("chordalSquare").size()

    //    fourCliqueSize
    val fourCliqueSize = pattern.get("fourClique").size()

    //    fourCliqueEdgeSize
    val fourCliqueEdgeSize = 0
//    pattern.get("fourCliqueEdge").size()

    //    total size
    val size =
      edgeSize + triangleSize +
        edgeSize + triangleEdgeSize +
        edgeSize + chordalSquareSize +
        edgeSize + fourCliqueSize +
        edgeSize + fourCliqueEdgeSize

    size
  }






}
