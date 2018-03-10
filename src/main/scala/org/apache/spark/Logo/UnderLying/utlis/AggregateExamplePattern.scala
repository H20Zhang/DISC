package org.apache.spark.Logo.UnderLying.utlis

import scala.collection.mutable

class AggregateExamplePattern(data: String,h1:Int=8,h2:Int=8) {


  val pattern = new ExamplePattern(data,h1,h2)

  lazy val triangle = {
    pattern.pattern("triangle").rdd().mapPartitions({
      f =>

        val longMap = new mutable.LongMap[Int]()
        while (f.hasNext){
          val p = f.next()
          val key = (p(0).toLong << 32) | (p(1) & 0xffffffffL)

          val v = longMap.getOrNull(key)
          if (v != null) {
            longMap.update(key,v+1)
          } else {
            longMap.put(key, 1)
          }
        }
        longMap.iterator
    },true).reduceByKey(_ + _).count()
  }


//  def pattern(name:String)  ={
//    name match {
//      case "triangle" => triangleIntersectionVersion
//      case "chordalSquare" => chordalSquareFast
//      case "square" => squareIntersectionVerificationFast
//      case "debug" => houseIntersectionFastNoSymmetryNew
//      case "house" => houseIntersectionFast
//      case "houseF" => houseIntersectionF
//      case "threeTriangle" => threeTriangleFast
//      case "threeTriangleF" => threeTriangleF
//      case "trianglePlusOneEdge" => trianglePlusOneEdge
//      case "trianglePlusTwoEdgeF" => trianglePlusTwoEdgeF
//      case "trianglePlusWedge" => trianglePlusWedge
//      case "squarePlusOneEdgeF" => squarePlusOneEdge
//      case "near5Clique" => near5Clique
//      case "chordalRoof" => chordalRoof
//    }
//  }


}
