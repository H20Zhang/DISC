package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog.{
  Attribute,
  AttributeID,
  RelationID
}
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.Method
import org.apache.spark.disc.optimization.Rule
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.disc.optimization.LogicalRule

trait LogicalPlan extends Serializable {
  val catalog = Catalog.defaultCatalog()
  val outputSchema: RelationSchema
  lazy val outputRelationString =
    s"${outputSchema.name}${outputSchema.attrs.mkString("(", ", ", ")")}"
  def optimize(): LogicalPlan
  def phyiscalPlan(): PhysicalPlan
  def getChildren(): Seq[LogicalPlan]
  def prettyString(): String = {

    if (getChildren().nonEmpty) {
      val childrenString =
        getChildren()
          .map(child => s"${child.prettyString()}\n")
          .reduce(_ + _)
          .dropRight(1)
          .split("\n")
          .map(str => s"\t${str}\n")
          .reduce(_ + _)
          .dropRight(1)

      s"-${selfString()}->${outputRelationString}\n${childrenString}"
    } else {
      s"-${selfString()}->${outputRelationString}"
    }
  }

  def selfString(): String = {
    s"unknown"
  }
}

class RuleNotMatchedException(rule: Rule)
    extends Exception(s"Not Supported Plan Type:${rule.getClass}") {}

abstract class Scan(schema: RelationSchema) extends LogicalPlan {
  def getChildren(): Seq[LogicalPlan] = {
    Seq[LogicalPlan]()
  }

  override val outputSchema = schema

  override def selfString(): String = {
    s"Scan(schema:${schema})"
  }

}

case class UnOptimizedScan(schema: RelationSchema) extends Scan(schema) {

  override def optimize(): LogicalPlan = {
    val memoryData = catalog.getMemoryStore(schema.id.get)
    val diskData = catalog.getDiskStore(schema.id.get)

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

  override def selfString(): String = {
    s"UnOptimizedScan(schema:${schema})"
  }
}

abstract class Filter(child: LogicalPlan) extends LogicalPlan {
  def getChildren(): Seq[LogicalPlan] = {
    Seq(child)
  }

  override val outputSchema = child.outputSchema

  override def selfString(): String = {
    s"Filter(schema:${outputSchema})"
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
    RelationSchema.tempSchemaWithAttrIds(attrIDs)
  }
}

case class UnOptimizedJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {

  def optimize(): LogicalPlan = {
    val inputs = childrenOps.map(_.optimize())
    import Method._
    conf.method match {
      case UnOptimizedHCube => UnCostOptimizedHCubeJoin(inputs)
      case PushHCube        => CostOptimizedPushHCubeJoin(inputs)
      case PullHCube        => CostOptimizedPullHCubeJoin(inputs)
      case MergedHCube      => CostOptimizedMergedHCubeJoin(inputs)
      case Factorize        => CostOptimizedHCubeFactorizedJoin(inputs)
      case ADJ              => CostOptimizedAdaptiveJoin(inputs)
      case CacheHCube       => CostOptimizedHCubeCachedJoin(inputs)
      case _                => throw new Exception(s"not such method supported ${conf.method}")
    }
  }

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("UnOptimized Join --- not supported")
  }
}
