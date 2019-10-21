package utils

import org.apache.spark.adj.utils.misc.WebGraphConverter

class WebGraphConverterTest extends SparkFunSuite {

  test("calOffset") {
    val converter = new WebGraphConverter
    val input = "/Volumes/NAS/Downloads/enwiki-2013-nat"
    converter.calculateOffSet(input)
  }
}
