//package graphFrameApplication.patternMatching
//
//
//import org.graphframes.{GraphFrame, examples}
//import org.apache.log4j.Logger
//
//
//class UnanchoredPattern(g:GraphFrame) {
//
//
//
//
//
//  val trianglePattern = "(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)"
//  val squarePattern = "(a)-[]->(b); (b)-[]->(c); (c)-[]->(d); (d)-[]->(a)"
//  val chordalSquarePattern = "(a)-[]->(b); (b)-[]->(c); (c)-[]->(d); (d)-[]->(a); (a)-[]->(c)"
//  val logger = Logger.getLogger(this.getClass)
//
//
//
//  def test(): Unit ={
//
//    //finding triangle pattern
//    logger.info("executing finding triangle")
//    val triangle = g.find(trianglePattern)
//    triangle.show()
//
//    //finding triangle pattern
//    logger.info("executing finding triangle")
//    val square = g.find(squarePattern)
//    square.show()
//
//
//    //finding triangle pattern
//    logger.info("executing finding triangle")
//    val chordalSquare = g.find(chordalSquarePattern)
//    chordalSquare.show()
//  }
//
//  def showSchema(): Unit ={
//    //finding triangle pattern
//    logger.info("executing finding triangle")
//    val triangle = g.find(trianglePattern)
//    triangle.explain(true)
//
//    //finding triangle pattern
//    logger.info("executing finding triangle")
//    val square = g.find(squarePattern)
//    square.explain(true)
//
//
//    //finding triangle pattern
//    logger.info("executing finding triangle")
//    val chordalSquare = g.find(chordalSquarePattern)
//    chordalSquare.explain(true)
//  }
//
//}
