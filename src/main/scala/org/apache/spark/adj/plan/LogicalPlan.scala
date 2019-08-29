package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID}
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

trait LogicalPlan extends Serializable {
  var defaultTasks = 224

  val db = Catalog.defaultCatalog()

  def optimizedPlan():LogicalPlan
  def phyiscalPlan():PhysicalPlan
  def info():RelationSchema
  def getChildren():Seq[LogicalPlan]
}



case class Scan(name: String) extends LogicalPlan {
  override def getChildren(): Seq[LogicalPlan] = {
    Seq[LogicalPlan]()
  }

  override def optimizedPlan(): LogicalPlan = {
    val id = db.getRelationID(name)
    val memoryData = db.getMemoryStore(id)
    val diskData = db.getDiskStore(id)

    if (memoryData.isDefined){
      return new InMemoryScan(name)
    }

    if (diskData.isDefined){
      return new DiskScan(name)
    }

    throw new Exception(s"no data found for Relation:${name}")
  }

  override def info(): RelationSchema = db.getSchema(name)

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("not supported")
  }
}

case class Join(childrenOps: Seq[LogicalPlan]) extends LogicalPlan {

  val schemas = childrenOps.map(_.info())
  val attrIDs = schemas.flatMap(_.attrIDs).distinct

  def optimizedPlan(): LogicalPlan = {
    new UnOptimizedHCubeJoin(childrenOps.map(_.optimizedPlan()))
  }

  def getSchema():Seq[RelationSchema] = {
    schemas
  }

  def getAttributes():Seq[Attribute] = {
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







