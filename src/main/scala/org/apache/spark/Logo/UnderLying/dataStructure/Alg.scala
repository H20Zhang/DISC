package org.apache.spark.Logo.UnderLying.dataStructure

class Alg {

}

object BSearch {
  def interative[T](array: Array[T], value: T)(implicit arithmetic: Numeric[T]): Int = {
    var left: Int = 0;
    var right: Int = array.length - 1;
    while (right > left) {
      val mid = left + (right - left) / 2
      val comp = arithmetic.compare(array(mid), value)
      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid - 1;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  def interative(array: Array[Int], value: Int, _left:Int, _right:Int)(implicit arithmetic: Numeric[Int]): Int = {
    var left: Int = _left;
    var right: Int = _right;
    while (right > left) {
      val mid = left + (right - left) / 2
      val comp = arithmetic.compare(array(mid), value)
      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid - 1;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

}
