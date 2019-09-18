package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.optimization.comp.{
  EnumShareComputer,
  FactorizeOrderComputer,
  NonLinearShareComputer,
  OrderComputer
}
import org.apache.spark.adj.optimization.optimizer.{
  ADJOptimizer,
  CacheLeapFrogOptimizer
}
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.misc.Conf
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class UnCostOptimizedHCubeJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {
//  val defaultShare = Math
//    .pow(Conf.defaultConf().taskNum.toDouble, 1 / attrIDs.size.toDouble)
//    .ceil
//    .toInt
  val defaultShare = 1
  var share: Map[AttributeID, Int] = attrIDs.map(id => (id, defaultShare)).toMap
  var attrOrder: Array[AttributeID] = attrIDs.sorted.toArray

  override def phyiscalPlan(): PhysicalPlan = {
    PullHCubeLeapJoinExec(
      outputSchema,
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

case class CostOptimizedMergedHCubeJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int =
                                          Conf.defaultConf().getTaskNum())
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
    share = shareComputer.optimalShare()._1

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    MergedHCubeLeapJoinExec(
      outputSchema,
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

case class CostOptimizedPushHCubeJoin(childrenOps: Seq[LogicalPlan],
                                      task: Int =
                                        Conf.defaultConf().getTaskNum())
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()
  var numTask = Conf.defaultConf().taskNum

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
    share = shareComputer.optimalShare()._1

    numTask = Conf.defaultConf().taskNum

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PushHCubeLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      numTask
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class CostOptimizedPullHCubeJoin(childrenOps: Seq[LogicalPlan],
                                      task: Int =
                                        Conf.defaultConf().getTaskNum())
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
    share = shareComputer.optimalShare()._1

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PullHCubeLeapJoinExec(
      outputSchema,
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

case class CostOptimizedHCubeFactorizedJoin(childrenOps: Seq[LogicalPlan],
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
    share = shareComputer.optimalShare()._1

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PullFactorizedLeapJoinExec(
      outputSchema,
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

case class CostOptimizedHCubeCachedJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int = Conf.defaultConf().taskNum)
    extends Join(childrenOps) {
  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  var totalCacheSize = Conf.defaultConf().totalCacheSize
  var cacheSize: Array[Int] = Array()
  var keyAndValues: Seq[(Array[Int], Array[Int])] = Seq()
  val statistic = Statistic.defaultStatistic()
  var numTask = Conf.defaultConf().taskNum

  init()

  def init() = {

    //init statistic
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    //compute cache related parameters
    val temp = new CacheLeapFrogOptimizer(relations) genOptimalPlan ()
    attrOrder = temp._1
    keyAndValues = temp._2

    cacheSize = new Array[Int](keyAndValues.size)

    if (cacheSize.size > 2) {
      var i = 0
      while (i < cacheSize.size) {
        cacheSize(i) = totalCacheSize / (cacheSize.size - 2)
        i += 1
      }
    } else {
      var i = 0
      while (i < cacheSize.size) {
        cacheSize(i) = 0
        i += 1
      }
    }

    //compute share related parameters
    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()._1

    numTask = Conf.defaultConf().taskNum
    val catlog = Catalog.defaultCatalog()

    println(
      s"cachePos:${keyAndValues.map(f => (f._1.toSeq, f._2.toSeq))}, attrOrder:${attrOrder.toSeq}, cacheSize:${cacheSize.toSeq}"
    )

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PushHCubeCachedLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      keyAndValues,
      cacheSize,
      numTask
    )
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedException
  }
}

//case class OptimizedHCubeGHDJoin(childrenOps: Seq[LogicalPlan], task: Int = 4)
//    extends Join(childrenOps) {
//  override def optimizedPlan(): LogicalPlan = ???
//
//  override def phyiscalPlan(): PhysicalPlan = ???
//
//}

//TODO: finish the remaining part then debug
case class CostOptimizedAdaptiveJoin(childrenOps: Seq[LogicalPlan],
                                     task: Int = 4)
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
        CostOptimizedMergedHCubeJoin(
          query.map(UnOptimizedScan).map(_.optimizedPlan())
        )
      val physicalPlan = logicalPlan.phyiscalPlan()
      physicalPlan
    }

    MergedHCubeLeapJoinExec(
      outputSchema,
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
    val diskData = catalog.getDiskStore(id)

    DiskScanExec(schema, diskData.get)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class InMemoryScan(schema: RelationSchema) extends Scan(schema) {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = catalog.getMemoryStore(id)

    InMemoryScanExec(schema, memoryData.get)
  }

  override def optimizedPlan(): LogicalPlan = {
    throw new NotImplementedError()
  }

}
