package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.optimization.comp.{
  EnumShareComputer,
  FactorizeOrderComputer,
  NonLinearShareComputer,
  OrderComputer
}
import org.apache.spark.adj.optimization.optimizer.ADJOptimizer
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.misc.Conf
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class UnOptimizedHCubeJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {
  var share: Map[AttributeID, Int] = attrIDs.map(id => (id, defaultShare)).toMap
  var attrOrder: Array[AttributeID] = attrIDs.sorted.toArray

  override def phyiscalPlan(): PhysicalPlan = {
    PullHCubeLeapJoinExec(
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      share.values.product
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class OptimizedMergedHCubeJoin(childrenOps: Seq[LogicalPlan],
                                    task: Int = Conf.defaultConf().getTaskNum())
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
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

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShareAndLoadAndCost()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    MergedHCubeLeapJoinExec(
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      task
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class OptimizedPushHCubeJoin(childrenOps: Seq[LogicalPlan],
                                  task: Int = Conf.defaultConf().getTaskNum())
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
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

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShareAndLoadAndCost()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PushHCubeLeapJoinExec(
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      task
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class OptimizedPullHCubeJoin(childrenOps: Seq[LogicalPlan],
                                  task: Int = Conf.defaultConf().getTaskNum())
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
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

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShareAndLoadAndCost()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PullHCubeLeapJoinExec(
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      task
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }
}

case class OptimizedHCubeFactorizedJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int =
                                          Conf.defaultConf().getTaskNum())
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  var corePos: Int = 0
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val orderComputer = new FactorizeOrderComputer(schemas)
    val temp = orderComputer.optimalOrder()
    attrOrder = temp._1.toArray
    corePos = temp._2

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShareAndLoadAndCost()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PullFactorizedLeapJoinExec(
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      corePos,
      task
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

//TODO: finish this
case class OptimizedHCubeCachedJoin(childrenOps: Seq[LogicalPlan],
                                    task: Int = 4)
    extends Join(childrenOps) {
  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): PhysicalPlan = ???

}

//TODO: finish this
case class OptimizedHCubeGHDJoin(childrenOps: Seq[LogicalPlan], task: Int = 4)
    extends Join(childrenOps) {
  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): PhysicalPlan = ???

}

//TODO: finish the remaining part then debug
case class OptimizedAdaptiveJoin(childrenOps: Seq[LogicalPlan], task: Int = 4)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()
  var preMaterializeQuery: Seq[Seq[RelationSchema]] = Seq()
  var remainingRelations: Seq[RelationSchema] = Seq()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val adjOptimizer = new ADJOptimizer(relations)
    val temp = adjOptimizer.genOptimalPlan()

    share = temp._4
    attrOrder = temp._3
    preMaterializeQuery = temp._1
    remainingRelations = temp._2
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedException
  }

  override def phyiscalPlan(): PhysicalPlan = {
    val inputPhysicalPlans = preMaterializeQuery.map { query =>
      val logicalPlan =
        OptimizedMergedHCubeJoin(
          query.map(UnOptimizedScan).map(_.optimizedPlan())
        )
      val physicalPlan = logicalPlan.phyiscalPlan()
      physicalPlan
    }

    MergedHCubeLeapJoinExec(
      inputPhysicalPlans ++ remainingRelations
        .map(UnOptimizedScan)
        .map(_.optimizedPlan().phyiscalPlan()),
      share,
      attrOrder,
      task
    )
  }

}

case class DiskScan(schema: RelationSchema) extends Scan(schema) {
  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val diskData = db.getDiskStore(id)

    DiskScanExec(schema, diskData.get)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class InMemoryScan(schema: RelationSchema) extends Scan(schema) {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = db.getMemoryStore(id)

    InMemoryScanExec(schema, memoryData.get)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}
