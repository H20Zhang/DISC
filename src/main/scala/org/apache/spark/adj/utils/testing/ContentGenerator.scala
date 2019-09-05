package org.apache.spark.adj.utils.testing

import scala.io.Source
import scala.util.Random

object ContentGenerator {
  val prefix = "./examples/"
  val graphDataAdresses = Map(
    ("eu", "email-Eu-core.txt"),
    ("wikiV", "wikiV.txt"),
    ("debug", "debugData.txt")
  )

  def loadGraphContent(name: String) = {

    val rawFile = Source.fromFile(prefix + graphDataAdresses(name)).getLines()

    val table = rawFile
      .map { f =>
        var res: Array[Int] = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = splittedString.map(_.toInt)
        }
        res
      }
      .filter(f => f != null)
      .toArray

//      .filter(f => f(0) != f(1)).map(f => (f(0), f(1))).flatMap(f => Iterator(f, f.swap)).toArray.distinct.map(f => Array(f._1, f._2))

    table
  }

  def genCatersianProductContent(
    domainSize: Int,
    arity: Int,
    SampleRate: Double = 0.5
  ): Array[Array[Int]] = {
    var content = Range(0, domainSize).map(f => Array(f)).toArray

    for (i <- 0 until arity - 1) {
      content = Range(0, domainSize).flatMap { value =>
        content.map { tuple =>
          tuple :+ value
        }
      }.toArray
    }

    Random.setSeed(System.currentTimeMillis())
    content = Random.shuffle(content.toSeq).toArray
    content.filter(p => Random.nextDouble() < SampleRate)
  }

  def genRandomContent(cardinality: Int, artiy: Int) = {
    val table = Range(0, cardinality)
      .map { _ =>
        Range(0, artiy).map { _ =>
          Math.abs(Random.nextInt() % (2 * cardinality))
        }.toSeq
      }
      .distinct
      .toArray
      .map(_.toArray)

    table
  }

  def genIdentityContent(cardinality: Int, artiy: Int) = {

    val oneColumn = Range(0, cardinality)
    val table = oneColumn.map(v => Seq.fill(artiy)(v).toArray).toArray

    table
  }

  def genRandomUnaryContent(cardinality: Int) = {

    val table = Range(0, cardinality)
      .map { _ =>
        Math.abs(Random.nextInt() % (2 * cardinality))
      }
      .distinct
      .toArray

    table
  }
}
