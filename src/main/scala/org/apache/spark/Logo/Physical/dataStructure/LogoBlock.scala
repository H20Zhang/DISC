package org.apache.spark.Logo.Physical.dataStructure
import org.apache.spark.graphx.VertexId

import scala.reflect.ClassTag



trait LogoBlockRef

abstract class LogoBlock[A:ClassTag](val schema: LogoSchema, val metaData: LogoMetaData, val rawData:A) extends LogoBlockRef with Serializable{}

class RowLogoBlock[A:ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawData:Seq[A]) extends LogoBlock(schema, metaData, rawData){}


class FileBasedLogoBlock;
class CompressedLogoBlock[A:ClassTag, B:ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawEdge:Seq[B], crystals:Seq[(String,List[B])], rawAttr:A)


//
////special case for edge with one hole
//class TwoTupleOneHoleListLogoBlock(color:Int,  logos:List[(VertexId,VertexId)]) extends ListLogoBlock(
//  List(color),
//  LogoSchema(List((0,1)), List(0), List(0)),
//  logos
//)
//
////special case for edge with two hole
//class TwoTupleTwoHoleListLogoBlock(colors:(Int,Int), logos:List[(VertexId,VertexId)]) extends ListLogoBlock(
//  List(colors._1,colors._2),
//  LogoSchema(List((0,1)), List(0,1), List(0,1)),
//  logos
//)
//
//
////generic case for logos with one hole
//class OneHoleListLogoBlock(color:Int, schema: LogoSchema, logos:List[(VertexId,List[VertexId])]) extends ListLogoBlock(
//  List(color),
//  schema ,
//  logos
//)
//
////generic case for logos with two hole
//class TwoHoleListLogoBlock(colors:(Int,Int), schema: LogoSchema, schemaEdges:List[(String,String)], schemaHoles:List[String], logos:List[((VertexId,VertexId),List[VertexId])]) extends ListLogoBlock(
//  List(colors._1,colors._2),
//  schema,
//  logos
//)
//
////generic case for logos
//class ListListLogBlock(colors:List[Int], schema: LogoSchema, logos:List[List[VertexId]]) extends  ListLogoBlock(
//  colors,
//  schema,
//  logos
//)
//
//
//











