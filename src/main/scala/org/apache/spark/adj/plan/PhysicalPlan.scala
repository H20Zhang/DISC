package org.apache.spark.adj.plan

import org.apache.spark.adj.hcube.TupleHCubeBlock
import org.apache.spark.adj.database.Database.{Attribute, AttributeID, RelationID}
import org.apache.spark.adj.database.{Database, Relation}
import org.apache.spark.adj.optimization.ShareOptimizer

import scala.collection.mutable

//physical plan is the plan that describe the distributed execution process
class PhysicalPlan

class LeapFrogPlan(query:LogicalPlan, tasksNum:Int) extends PhysicalPlan {
  var share:Map[Attribute, Int] = _
  var attrOrder:IndexedSeq[Attribute] = _
}


