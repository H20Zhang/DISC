package org.apache.spark.Logo.dataStructure
import org.apache.spark.graphx.VertexId

import scala.reflect.ClassTag


case class LogoSchema(val schemaEdges:List[(Int,Int)], val mainHole:List[Int] ,val schemaHoles:List[Int]) {}

trait LogoBlock {
  val colors:List[Int]
  def getLogoSchema:LogoSchema
  def setLogoSchema(schema:LogoSchema)
  def partNums:Long
}

abstract class ListLogoBlock[A:ClassTag](val colors:List[Int], schema:LogoSchema, logos:List[A]) extends LogoBlock{

  private var _schema = this.schema
  private var _logos = this.logos


  override def getLogoSchema = _schema
  override def partNums: VertexId = _logos.length

  override def setLogoSchema(schema: LogoSchema): Unit = {
    _schema = schema
  }

  def setLogo(logos:List[A]): Unit ={
    _logos = logos
  }

  def getLogo = _logos
}

//special case for edge with one hole
class TwoTupleOneHoleListLogoBlock(color:Int,  logos:List[(VertexId,VertexId)]) extends ListLogoBlock(
  List(color),
  LogoSchema(List((0,1)), List(0), List(0)),
  logos
)

//special case for edge with two hole
class TwoTupleTwoHoleListLogoBlock(colors:(Int,Int), logos:List[(VertexId,VertexId)]) extends ListLogoBlock(
  List(colors._1,colors._2),
  LogoSchema(List((0,1)), List(0,1), List(0,1)),
  logos
)


//generic case for logos with one hole
class OneHoleListLogoBlock(color:Int, schema: LogoSchema, logos:List[(VertexId,List[VertexId])]) extends ListLogoBlock(
  List(color),
  schema ,
  logos
)

//generic case for logos with two hole
class TwoHoleListLogoBlock(colors:(Int,Int), schema: LogoSchema, schemaEdges:List[(String,String)], schemaHoles:List[String], logos:List[((VertexId,VertexId),List[VertexId])]) extends ListLogoBlock(
  List(colors._1,colors._2),
  schema,
  logos
)

//generic case for logos
class ListListLogBlock(colors:List[Int], schema: LogoSchema, logos:List[List[VertexId]]) extends  ListLogoBlock(
  colors,
  schema,
  logos
)














