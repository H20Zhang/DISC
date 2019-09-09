package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.optimization.comp.{
  EnumShareComputer,
  FactorizeOrderComputer,
  NonLinearShareComputer,
  OrderComputer
}
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.utils.misc.Conf

case class UnOptimizedHCubeJoin(childrenOps: Seq[LogicalPlan])
    extends LogicalPlan {
  val schemas = childrenOps.map(_.info())
  val attrIDs = schemas.flatMap(_.attrIDs).distinct

  var share: Map[AttributeID, Int] = attrIDs.map(id => (id, defaultShare)).toMap
  var attrOrder: Array[AttributeID] = attrIDs.toArray

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

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

case class OptimizedMergedHCubeJoin(childrenOps: Seq[LogicalPlan],
                                    task: Int = Conf.defaultConf().getTaskNum())
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

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

case class OptimizedPushHCubeJoin(childrenOps: Seq[LogicalPlan],
                                  task: Int = Conf.defaultConf().getTaskNum())
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

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

case class OptimizedPullHCubeJoin(childrenOps: Seq[LogicalPlan],
                                  task: Int = Conf.defaultConf().getTaskNum())
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

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

case class OptimizedHCubeFactorizedJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int =
                                          Conf.defaultConf().getTaskNum())
    extends LogicalPlan {

  val schemas = childrenOps.map(_.info())
  val attrIDs = schemas.flatMap(_.attrIDs).distinct

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  var corePos: Int = 0
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

  override def info(): RelationSchema = {
    RelationSchema("joinResult", attrIDs.map(db.getAttribute))
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps
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
