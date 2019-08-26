package utils

import utils.ContentGenerator.{graphDataAdresses, prefix}

import scala.io.Source
import scala.util.Random

object TestingHelper {


  def genGraphContent(name:String) = ContentGenerator.genGraphContent(name)

  def genRandomContent(cardinality:Int, artiy:Int) = ContentGenerator.genRandomContent(cardinality, artiy)

  def genIdentityContent(cardinality:Int, artiy:Int) = ContentGenerator.genIdentityContent(cardinality, artiy)
}
