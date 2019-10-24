package org.dsce.optimization.aggregate.deprecated.util

import cern.colt.matrix.impl.DenseDoubleMatrix2D
import com.joptimizer.optimizers.{LPOptimizationRequest, LPPrimalDualMethod}
import org.dsce.optimization.subgraph.deprecated.graph.Graph
import org.dsce.optimization.subgraph.deprecated.remover.SymmetryBreakingRule
import org.dsce.{GraphID, LogAble}

import scala.collection.mutable
import scala.math.BigDecimal.RoundingMode

object WidthCalculator extends LogAble {

  private lazy val graphIDWidthCatelog: mutable.HashMap[GraphID, Double] =
    mutable.HashMap()
  private lazy val graphIDfhtwCatelog: mutable.HashMap[GraphID, Double] =
    mutable.HashMap()
  private lazy val graphCatelog = GraphCatlog.getcatlog()

//  The fractional hypertree width of the graph
  def fhtw(g: Graph) = {

    val id = graphCatelog(g)

    graphIDfhtwCatelog.get(id) match {
      case Some(fhtw) => fhtw
      case None => {
        val GHDs = HyperTreeDecomposer.allGHDs(g)
        val ghdFractionalHyperNodeWidthPairs =
          GHDs.map(ghd => (ghd, ghd.fractionalHyperNodeWidth()))
        val fhtw = ghdFractionalHyperNodeWidthPairs.map(_._2).min
        graphIDfhtwCatelog(id) = fhtw

        fhtw
      }
    }
  }

//  The fractional hypertree width under constraint

  def fhtwUnderConstraint(g: Graph, rules: Seq[SymmetryBreakingRule]) = {

    logger.debug(s"g:${g} \n rules:${rules}")

    var GHDs = HyperTreeDecomposer.allGHDs(g)

    GHDs = GHDs.filter { t =>
      val hv = t.HV
      rules.forall { rule =>
        hv.exists { n =>
          n.g.containNodes(rule.isomorphismNodes)
        }
      }
    }

    val ghdFractionalHyperNodeWidthPairs =
      GHDs.map(ghd => (ghd, ghd.fractionalHyperNodeWidth()))
    val fhtw = ghdFractionalHyperNodeWidthPairs.map(_._2).min

    fhtw
  }

//  The AGM width of the graph
  def width(g: Graph) = {
    val id = graphCatelog(g)

    graphIDWidthCatelog.get(id) match {
      case Some(width) => width
      case None => {
        val fractionalWidth = _width(g)
        graphIDWidthCatelog(id) = fractionalWidth
        fractionalWidth
      }
    }
  }

  //  compute the fractional width of the graph
  def _width(g: Graph) = {
    val edges = g.E()
    val nodes = g.V()

    val c = edges.map(e => 1.0).toArray
    val G = new DenseDoubleMatrix2D(nodes.size, c.size)

    for (i <- 0 until nodes.size) {
      for (j <- 0 until edges.size) {
        if (edges(j).productIterator.contains(nodes(i))) {
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
