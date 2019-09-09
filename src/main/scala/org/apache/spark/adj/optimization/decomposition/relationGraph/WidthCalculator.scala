package org.apache.spark.adj.optimization.decomposition.relationGraph

import cern.colt.matrix.impl.DenseDoubleMatrix2D
import com.joptimizer.optimizers.{LPOptimizationRequest, LPPrimalDualMethod}
import org.apache.spark.adj.optimization.decomposition.graph.Graph._

import scala.collection.mutable
import scala.math.BigDecimal.RoundingMode

object WidthCalculator {

  //  compute the fractional width of the graph
  def width(g: RelationGraph) = {
    val edges = g.E()
    val nodes = g.V()

    val c = edges.map(e => 1.0).toArray
    val G = new DenseDoubleMatrix2D(nodes.size, c.size)

    for (i <- 0 until nodes.size) {
      for (j <- 0 until edges.size) {
        if (edges(j).attrs.contains(nodes(i))) {
          G.set(i, j, -1.0)
        } else {
          G.set(i, j, 0.0)
        }
      }
    }

    val h = Array.fill(nodes.size)(-1.0)
    val lb = Array.fill(c.size)(0.0)
    val ub = Array.fill(c.size)(1.01)

    val or = new LPOptimizationRequest

    val opt = new LPPrimalDualMethod
    or.setC(c)
    or.setG(G)
    or.setH(h)
    or.setLb(lb)
    or.setUb(ub)
    or.setDumpProblem(false)
    opt.setLPOptimizationRequest(or)
    opt.optimize()

    val rawsol = opt.getOptimizationResponse.getSolution

    val sol = rawsol.map(
      f =>
        BigDecimal.valueOf(f).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    )

    sol.sum
  }
}
