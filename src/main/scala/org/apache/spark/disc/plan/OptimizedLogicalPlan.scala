package org.apache.spark.disc.plan

import org.apache.spark.disc.optimization.cost_based.comp.{
  EnumShareComputer,
  FactorizeOrderComputer,
  OrderComputer
}

import org.apache.spark.disc.optimization.cost_based.stat.Statistic
import org.apache.spark.disc.catlog.Catalog.AttributeID
import org.apache.spark.disc.catlog.{Catalog, Schema}
import org.apache.spark.disc.util.misc.Conf
import sun.reflect.generics.reflectiveObjects.NotImplementedException

case class UnCostOptimizedHCubeJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {
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

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class CostOptimizedMergedHCubeJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int =
                                          Conf.defaultConf().NUM_PARTITION)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()
  var numTask = Conf.defaultConf().NUM_PARTITION

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

    numTask = Conf.defaultConf().NUM_PARTITION
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
      share.values.product
    )
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }

}

case class CostOptimizedPushHCubeJoin(childrenOps: Seq[LogicalPlan],
                                      task: Int =
                                        Conf.defaultConf().NUM_PARTITION)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()
  var numTask = Conf.defaultConf().NUM_PARTITION

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

    numTask = Conf.defaultConf().NUM_PARTITION

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

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }

}

case class CostOptimizedPullHCubeJoin(childrenOps: Seq[LogicalPlan],
                                      task: Int =
                                        Conf.defaultConf().NUM_PARTITION)
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

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }
}

case class CostOptimizedHCubeFactorizedJoin(childrenOps: Seq[LogicalPlan],
                                            task: Int =
                                              Conf.defaultConf().NUM_PARTITION)
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

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }

}

case class DiskScan(schema: Schema) extends Scan(schema) {
  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val diskData = catalog.getDiskStore(id)

    DiskScanExec(schema, diskData.get)
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override def selfString(): String = {
    s"DiskScan(schema:${schema})"
  }

}

case class InMemoryScan(schema: Schema) extends Scan(schema) {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = catalog.getMemoryStore(id)

    InMemoryScanExec(schema, memoryData.get)
  }

  override def optimize(): LogicalPlan = {
    this
  }

  override def selfString(): String = {
    s"InMemoryScan(schema:${schema})"
  }

}
