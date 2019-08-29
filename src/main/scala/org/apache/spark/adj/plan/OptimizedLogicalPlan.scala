package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.optimization.{OrderComputer, ShareComputer}
import org.apache.spark.adj.optimization.utils.Statistic

//trait OptimizedLogicalPlan extends LogicalPlan {
//  val statistic = new Statistic
//
//  def physicalPlan():PhysicalPlan
//  def execute():Relation
//
//  def getCardinalities():Map[RelationSchema, Long] = ???
//  def getDegrees() = ???
//  def optimize() = ???
//}

case class UnOptimizedHCubeJoin(childrenOps: Seq[LogicalPlan])
    extends LogicalPlan {
  val schemas = childrenOps.map(_.info())
  val attrIDs = schemas.flatMap(_.attrIDs).distinct

  var share: Map[AttributeID, Int] = attrIDs.map(id => (id, defaultShare)).toMap
  var attrOrder: Array[AttributeID] = attrIDs.toArray

  override def phyiscalPlan(): PhysicalPlan = {
    HCubeLeapJoinExec(getChildren().map(_.phyiscalPlan()), share, attrOrder)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

//TODO: debug this class and all the class involved in this class
case class OptimizedHCubeJoin(childrenOps: Seq[LogicalPlan], task: Int = 4)
    extends LogicalPlan {

  val schemas = childrenOps.map(_.info())
  val attrIDs = schemas.flatMap(_.attrIDs).distinct

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.info()).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val orderComputer = new OrderComputer(schemas)
    attrOrder = orderComputer.optimalOrder()

    val shareComputer = new ShareComputer(schemas, task)
    share = shareComputer.optimalShare()
  }

  override def phyiscalPlan(): PhysicalPlan = {
    HCubeLeapJoinExec(getChildren().map(_.phyiscalPlan()), share, attrOrder)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

case class DiskScan(schema: RelationSchema) extends LogicalPlan {
  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val diskData = db.getDiskStore(id)

    DiskScanExec(schema, diskData.get)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override def info(): RelationSchema = {
    schema
  }

  override def getChildren(): Seq[LogicalPlan] = Seq()
}

case class InMemoryScan(schema: RelationSchema) extends LogicalPlan {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = db.getMemoryStore(id)

    InMemoryScanExec(schema, memoryData.get)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override def info(): RelationSchema = {
    schema
  }

  override def getChildren(): Seq[LogicalPlan] = Seq()
}

//TODO: finish this
case class OptimizedHCubeFactorizedJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int = 4)
    extends LogicalPlan {
  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): PhysicalPlan = ???

  override def info(): RelationSchema = ???

  override def getChildren(): Seq[LogicalPlan] = ???
}

//TODO: finish this
case class OptimizedHCubeCachedJoin(childrenOps: Seq[LogicalPlan],
                                    task: Int = 4)
    extends LogicalPlan {
  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): PhysicalPlan = ???

  override def info(): RelationSchema = ???

  override def getChildren(): Seq[LogicalPlan] = ???
}

//TODO: finish this
case class OptimizedHCubeGHDJoin(childrenOps: Seq[LogicalPlan], task: Int = 4)
    extends LogicalPlan {
  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): PhysicalPlan = ???

  override def info(): RelationSchema = ???

  override def getChildren(): Seq[LogicalPlan] = ???
}

//TODO: finish this
case class OptimizedAdaptiveJoin(childrenOps: Seq[LogicalPlan], task: Int = 4)
    extends LogicalPlan {
  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): PhysicalPlan = ???

  override def info(): RelationSchema = ???

  override def getChildren(): Seq[LogicalPlan] = ???
}
