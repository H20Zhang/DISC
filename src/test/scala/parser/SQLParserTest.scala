package parser

import org.apache.spark.adj.parser.sql.SQLParser
import org.scalatest.FunSuite

class SQLParserTest extends FunSuite{

  test("basic"){

    val parser = new SQLParser
    val sqlCode ="select * from R1 where a='a'"
    val ops = parser.parseSql(sqlCode)
    println(ops)
  }
}
