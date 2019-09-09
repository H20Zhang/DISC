package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.{
  Attribute,
  AttributeID,
  RelationID
}
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.Method
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

trait LogicalPlan extends Serializable {
  var defaultTasks = 224
  val defaultShare = 2

  val db = Catalog.defaultCatalog()

  def optimizedPlan(): LogicalPlan
  def phyiscalPlan(): PhysicalPlan
  def info(): RelationSchema
  def getChildren(): Seq[LogicalPlan]
}

case class Scan(schema: RelationSchema) extends LogicalPlan {
  override def getChildren(): Seq[LogicalPlan] = {
    Seq[LogicalPlan]()
  }

  override def optimizedPlan(): LogicalPlan = {
    val memoryData = db.getMemoryStore(schema.id.get)
    val diskData = db.getDiskStore(schema.id.get)

    if (memoryData.isDefined) {
      return InMemoryScan(schema)
    }

    if (diskData.isDefined) {
      return DiskScan(schema)
    }

    throw new Exception(s"no data found for Relation:$schema")
  }

  override def info(): RelationSchema = schema

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("not supported")
  }
}

case class Join(childrenOps: Seq[LogicalPlan]) extends LogicalPlan {

  val schemas = childrenOps.map(_.info())
  val attrIDs = schemas.flatMap(_.attrIDs).distinct
  val conf = Conf.defaultConf()

  def optimizedPlan(): LogicalPlan = {
//    UnOptimizedHCubeJoin(childrenOps.map(_.optimizedPlan()))

    val inputs = childrenOps.map(_.optimizedPlan())
    import Method._
    conf.method match {
      case PushHCube   => OptimizedPushHCubeJoin(inputs)
      case PullHCube   => OptimizedPullHCubeJoin(inputs)
      case MergedHCube => OptimizedMergedHCubeJoin(inputs)
      case Factorize   => OptimizedHCubeFactorizedJoin(inputs)
      case _           => throw new Exception(s"not such method supported ${conf.method}")
    }
  }

  def getSchema(): Seq[RelationSchema] = {
    schemas
  }

  def getAttributes(): Seq[Attribute] = {
    getSchema().flatMap(_.attrs).distinct
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("not supported")
  }
}
