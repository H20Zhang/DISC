package hzhang.test.exp.utils

import org.apache.spark.adj.deprecated.execution.rdd.loader.DataLoader

class HyperCubeCommPattern(data: String, h1:Int=6, h2:Int=6) extends Serializable {


  def pattern(name:String)  = {
    name match {
      case "hyberCubeTriangle" => TriangleHyberCube
      case "hyberCubeFourClique" => FourCliqueHyberCube
      case "hyberCubeChordalSquare" => chordalSquareHyberCube
      case "hyberCubeSquare" => SquareHyberCube
      case "hyberCubeHouse" => houseHyberCube
      case "hyberCubeThreeTriangle" => threeTriangleHyberCube
      case "hyberCubeNear5Clique" => near5CliqueHyberCube
      case _ => null
    }
  }


  lazy val rawEdge = {
    new DataLoader(data) rawEdgeRDD
  }


  def TriangleHyberCube = {
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val leftEdge = edge.flatMap{f =>
      0 until h1 map(t => ((f._1%h1,f._2%h1,t),f))
    }

    val centerEdge = edge.flatMap{f =>
      0 until h1 map(t => ((f._1%h1,t,f._2%h1),f))
    }

    val rightEdge = edge.flatMap{f =>
      0 until h1 map(t => ((t,f._1%h1,f._2%h1),f))
    }

    val hyberCube = leftEdge ++ centerEdge ++ rightEdge
    hyberCube.groupByKey()
  }


//  t => ((f._1%h1,f._2%h1,t),f)
  def FourCliqueHyberCube = {
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val edge1 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,f._2%h1,t,w),f))
      }
    }

    val edge2 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,t,f._2%h1,w),f))
      }
    }

    val edge3 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,f._1%h1,f._2%h1,w),f))
      }
    }

    val edge4 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,f._1%h1,w,f._2%h1),f))
      }
    }

    val edge5 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,t,w,f._2%h1),f))
      }
    }

    val edge6 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,w,f._1%h1,f._2%h1),f))
      }
    }

    val hyberCube = edge1 ++ edge2 ++ edge3 ++ edge4 ++ edge5 ++ edge6
    hyberCube.groupByKey()
  }

  def SquareHyberCube = {
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val edge1 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,f._2%h1,t,w),f))
      }
    }

//    val edge2 = edge.flatMap{f =>
//      0 until h1 flatMap { t =>
//        0 until h1 map(w => ((f._1%h1,t,f._2%h1,w),f))
//      }
//    }

    val edge3 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,f._1%h1,f._2%h1,w),f))
      }
    }

//    val edge4 = edge.flatMap{f =>
//      0 until h1 flatMap { t =>
//        0 until h1 map(w => ((t,f._1%h1,w,f._2%h1),f))
//      }
//    }

    val edge5 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,t,w,f._2%h1),f))
      }
    }

    val edge6 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,w,f._1%h1,f._2%h1),f))
      }
    }

    val hyberCube = edge1 ++ edge3 ++ edge5 ++ edge6
    hyberCube.groupByKey()
  }

  def chordalSquareHyberCube = {
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val edge1 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,f._2%h1,t,w),f))
      }
    }

//    val edge2 = edge.flatMap{f =>
//      0 until h1 flatMap { t =>
//        0 until h1 map(w => ((f._1%h1,t,f._2%h1,w),f))
//      }
//    }

    val edge3 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,f._1%h1,f._2%h1,w),f))
      }
    }

    val edge4 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,f._1%h1,w,f._2%h1),f))
      }
    }

    val edge5 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((f._1%h1,t,w,f._2%h1),f))
      }
    }

    val edge6 = edge.flatMap{f =>
      0 until h1 flatMap { t =>
        0 until h1 map(w => ((t,w,f._1%h1,f._2%h1),f))
      }
    }

    val hyberCube = edge1 ++ edge3 ++ edge4 ++ edge5 ++ edge6
    hyberCube.groupByKey()
  }

  def threeTriangleHyberCube = {
//    val h = Array(5, 1, 9, 1, 5)
    val h = Array(9, 2, 10, 3, 5)
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val edge1 = edge.flatMap{f =>
      0 until h(2) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((f._1 % h1, f._2 % h1, t, w, q), f))
        }
      }
    }

    val edge2 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((t, f._1 % h1, f._2 % h1, w, q), f))
        }
      }
    }

    val edge3 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(4) map (q => ((t, w, f._1 % h1, f._2 % h1, q), f))
        }
      }
    }

    val edge4 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(2) map (q => ((t, w, q, f._1 % h1, f._2 % h1), f))
        }
      }
    }

    val edge5 = edge.flatMap{f =>
      0 until h(1) flatMap { t =>
        0 until h(2) flatMap { w =>
          0 until h(3) map (q => ((f._1 % h1, t, w, q, f._2 % h1), f))
        }
      }
    }

    val edge6 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(3) map (q => ((t, w, f._1 % h1, q, f._2 % h1), f))
        }
      }
    }

    val edge7 = edge.flatMap{f =>
      0 until h(1) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((f._1 % h1, t,f._2 % h1, w, q ), f))
        }
      }
    }

    val edge8 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(2) flatMap { w =>
          0 until h(3) map (q => ((t, f._1 % h1, w, q, f._2 % h1), f))
        }
      }
    }



    val hyberCube = edge1 ++ edge2 ++ edge3 ++ edge4 ++ edge5 ++ edge6 ++ edge7


    hyberCube.groupByKey()
  }

  def houseHyberCube = {
//    val h = Array(3, 2, 5, 2, 4)
    val h = Array(5, 3, 10, 3, 6)
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val edge1 = edge.flatMap{f =>
      0 until h(2) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((f._1 % h1, f._2 % h1, t, w, q), f))
        }
      }
    }

    val edge2 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((t, f._1 % h1, f._2 % h1, w, q), f))
        }
      }
    }

    val edge3 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(4) map (q => ((t, w, f._1 % h1, f._2 % h1, q), f))
        }
      }
    }

    val edge4 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(2) map (q => ((t, w, q, f._1 % h1, f._2 % h1), f))
        }
      }
    }

    val edge5 = edge.flatMap{f =>
      0 until h(1) flatMap { t =>
        0 until h(2) flatMap { w =>
          0 until h(3) map (q => ((f._1 % h1, t, w, q, f._2 % h1), f))
        }
      }
    }

    val edge6 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(3) map (q => ((t, w, f._1 % h1, q, f._2 % h1), f))
        }
      }
    }

    val edge7 = edge.flatMap{f =>
      0 until h(1) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((f._1 % h1, t,f._2 % h1, w, q ), f))
        }
      }
    }

    val edge8 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(2) flatMap { w =>
          0 until h(3) map (q => ((t, f._1 % h1, w, q, f._2 % h1), f))
        }
      }
    }




    val hyberCube = edge1 ++ edge2 ++ edge3 ++ edge4 ++ edge5 ++ edge6


    hyberCube.groupByKey()
  }

  def near5CliqueHyberCube = {
//    val h = Array(3, 3, 5, 1, 5)
    val h = Array(5, 3, 10, 2, 9)
    val edge = rawEdge.map(f => (f._1(0),f._1(1)))
    val edge1 = edge.flatMap{f =>
      0 until h(2) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((f._1 % h1, f._2 % h1, t, w, q), f))
        }
      }
    }

    val edge2 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((t, f._1 % h1, f._2 % h1, w, q), f))
        }
      }
    }

    val edge3 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(4) map (q => ((t, w, f._1 % h1, f._2 % h1, q), f))
        }
      }
    }

    val edge4 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(2) map (q => ((t, w, q, f._1 % h1, f._2 % h1), f))
        }
      }
    }

    val edge5 = edge.flatMap{f =>
      0 until h(1) flatMap { t =>
        0 until h(2) flatMap { w =>
          0 until h(3) map (q => ((f._1 % h1, t, w, q, f._2 % h1), f))
        }
      }
    }

    val edge6 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(1) flatMap { w =>
          0 until h(3) map (q => ((t, w, f._1 % h1, q, f._2 % h1), f))
        }
      }
    }

    val edge7 = edge.flatMap{f =>
      0 until h(1) flatMap { t =>
        0 until h(3) flatMap { w =>
          0 until h(4) map (q => ((f._1 % h1, t,f._2 % h1, w, q ), f))
        }
      }
    }

    val edge8 = edge.flatMap{f =>
      0 until h(0) flatMap { t =>
        0 until h(2) flatMap { w =>
          0 until h(3) map (q => ((t, f._1 % h1, w, q, f._2 % h1), f))
        }
      }
    }



    val hyberCube = edge1 ++ edge2 ++ edge3 ++ edge4 ++ edge5 ++ edge6 ++ edge7 ++ edge8


    hyberCube.groupByKey()
  }




}
