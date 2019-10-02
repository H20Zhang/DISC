package optimization

import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.costBased.comp.FactorizeOrderComputer
import org.scalatest.FunSuite

class FactorizeOrderComputerTest extends FunSuite {

  test("computer") {

    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))
    val schemaR3 = RelationSchema("R3", Seq("B", "D"))
    val schemaR4 = RelationSchema("R4", Seq("C", "E"))
    val schemaR5 = RelationSchema("R5", Seq("D", "E"))

    val schemas =
      Seq(schemaR0, schemaR1, schemaR2, schemaR3, schemaR4, schemaR5)
    schemas.foreach(_.register())
    val computer = new FactorizeOrderComputer(schemas)

    println(s"optimal order:${computer.optimalOrder()}")
  }
}
