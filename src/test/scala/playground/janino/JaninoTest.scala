package playground.janino

import org.codehaus.janino.{ClassBodyEvaluator, ExpressionEvaluator}
import org.scalatest.FunSuite

class JaninoTest extends FunSuite {

  test("expressionEvaluator") {
    val ee = new ExpressionEvaluator()
    // The expression will have two "int" parameters: "a" and "b".// The expression will have two "int" parameters: "a" and "b".
    ee.setParameters(
      Array[String]("a", "b"),
      Array[Class[_]](classOf[Int], classOf[Int])
    )

    // And the expression (i.e. "result") type is also "int".
    ee.setExpressionType(classOf[Int])

    // And now we "cook" (scan, parse, compile and load) the fabulous expression.
    ee.cook("a + b")

    // Eventually we evaluate the expression - and that goes super-fast.
    val theArray = Array(19.asInstanceOf[AnyRef], 23.asInstanceOf[AnyRef])
    val result = ee.evaluate(theArray).asInstanceOf[Int]

    println(s"result:${result}")
  }

  test("classBodyEvaluator") {

    val hello_world =
      s"""
         |public void hello_world() { 
         |  System.out.println("hello world1!");
         |}
         |""".stripMargin
    val cbe = new ClassBodyEvaluator
    cbe.setClassName("JaninoClass")
    cbe.setExtendedClass(classOf[AbstractJaninoClass])
    cbe.cook(hello_world)
    val c = cbe.getClazz
    val op = c.newInstance.asInstanceOf[AbstractJaninoClass]
    op.hello_world()
  }

  test("janino-import") {}

  test("triangleEnumeration") {}
}
