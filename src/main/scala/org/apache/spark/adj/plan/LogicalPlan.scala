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
  val outputSchema: RelationSchema

  def optimizedPlan(): LogicalPlan
  def phyiscalPlan(): PhysicalPlan

  def getChildren(): Seq[LogicalPlan]
}

abstract class Scan(schema: RelationSchema) extends LogicalPlan {
  def getChildren(): Seq[LogicalPlan] = {
    Seq[LogicalPlan]()
  }

  override val outputSchema = schema
}

case class UnOptimizedScan(schema: RelationSchema) extends Scan(schema) {

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

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("not supported")
  }
}

abstract class Join(childrenOps: Seq[LogicalPlan]) extends LogicalPlan {
  val schemas = childrenOps.map(_.outputSchema)
  val attrIDs = schemas.flatMap(_.attrIDs).distinct
  val conf = Conf.defaultConf()

  def getSchema(): Seq[RelationSchema] = {
    schemas
  }

  def getAttributes(): Seq[Attribute] = {
    getSchema().flatMap(_.attrs).distinct
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps

  override val outputSchema: RelationSchema = {
    val catalog = Catalog.defaultCatalog()
    val schema = RelationSchema(
      s"Temp-R${catalog.nextRelationID()}",
      attrIDs.map(db.getAttribute)
    )
    catalog.add(schema)
    schema
  }
}

case class UnOptimizedJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {

  def optimizedPlan(): LogicalPlan = {
    val inputs = childrenOps.map(_.optimizedPlan())
    import Method._
    conf.method match {
      case UnOptimizedHCube => UnOptimizedHCubeJoin(inputs)
      case PushHCube        => OptimizedPushHCubeJoin(inputs)
      case PullHCube        => OptimizedPullHCubeJoin(inputs)
      case MergedHCube      => OptimizedMergedHCubeJoin(inputs)
      case Factorize        => OptimizedHCubeFactorizedJoin(inputs)
      case _                => throw new Exception(s"not such method supported ${conf.method}")
    }
  }

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("not supported")
  }
}
