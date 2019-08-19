package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Database.Attribute
import org.apache.spark.adj.database.Query
import org.apache.spark.adj.plan.optimization.ShareOptimizer



class LeapFrogPlan(query:Query, tasks:Int) {
  var share:Map[Attribute, Int] = _
  var attrOrder:IndexedSeq[Attribute] = _

  def optimize() = ???

  def optimizeOrder() = ???

  def optimizeShare() = {
    val shareOptimizer = new ShareOptimizer(query, tasks)
    share = shareOptimizer.compute()
  }
}
