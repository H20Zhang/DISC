package org.apache.spark.disc.execution.hcube

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

/*partition the relation according to the space defined by share,
  the i-th share decide how much "share" the domain of relation on i-th local attribute will be splited
 */
class HCubePartitioner(shareSpaceVector: Array[Int]) extends Partitioner {
  val artiy = shareSpaceVector.size
  val productFactor = 1 +: Range(1, artiy)
    .map(i => shareSpaceVector.dropRight(artiy - i).product)
    .toArray
  val _numPartitions = shareSpaceVector.product

  def nonNegativeModForLong(x: Long, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0) toInt
  }

  override def numPartitions: Int = _numPartitions

  override def getPartition(key: Any): Int = key match {
    case array: Array[Int] => {
      var i = 0
      var hashValue = 0
      while (i < artiy) {
        val ithHashValue = Utils.nonNegativeMod(array(i), shareSpaceVector(i))
        hashValue += (ithHashValue * productFactor(i))
        i += 1
      }
      hashValue
    }
    case array: Array[Long] => {
      var i = 0
      var hashValue = 0
      while (i < artiy) {
        val ithHashValue = nonNegativeModForLong(array(i), shareSpaceVector(i))
        hashValue += (ithHashValue * productFactor(i))
        i += 1
      }
      hashValue
    }
    case _ => throw new Exception("such key are not supported")
  }

  def getShare(hashValue: Int): Array[Int] = {
    var i = artiy - 1
    val shareArray = new Array[Int](artiy)
    var remain = hashValue
    while (i >= 0) {
      shareArray(i) = remain / productFactor(i)
      remain = remain % productFactor(i)
      i -= 1
    }
    shareArray
  }
}
