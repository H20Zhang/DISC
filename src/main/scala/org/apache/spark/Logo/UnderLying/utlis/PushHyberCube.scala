package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.UnderLying.Loader.EdgeLoader

class PushHyberCube (data: String,h1:Int=6,h2:Int=6) extends Serializable {


  lazy val rawEdge = {
    new EdgeLoader(data) rawEdgeRDD
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
    hyberCube.groupByKey().count()
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
    hyberCube.groupByKey().count()
  }




}
