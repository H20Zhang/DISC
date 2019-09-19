package org.apache.spark.adj.utils.exp

import java.util.{Timer, TimerTask}
import java.util.concurrent.{CancellationException, FutureTask}

import org.apache.spark.adj.database.{Catalog, Query, Relation, RelationSchema}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.utils.misc.Conf.Method
import org.apache.spark.adj.utils.misc.{Conf, SparkSingle}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Failure

class ExpExecutor(data: String,
                  query: String,
                  timeout: Int,
                  isCommOnly: Boolean) {

  def execute() = {

    SparkSingle.appName =
      s"ADJ-data:${data}-query:${query}-timeout:${timeout}-isCommOnly:${isCommOnly}-method:${Conf.defaultConf().method}"

    val expQuery = new ExpQuery(data)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val someTask = FutureTask.schedule(timeout seconds) {
      SparkSingle.getSparkContext().cancelAllJobs()
      println("timeout")
    }

//    val future = Future {

    if (Conf.defaultConf().method != Method.SPARKSQL) {
      if (isCommOnly) {
        Query.showPlan(expQuery.getQuery(query))
      } else {
        Query.countQuery(expQuery.getQuery(query))
      }
    } else {
      val sparkExecutor = new SparkSQLExecutor(
        expQuery.getRelations(query).map(_.schema)
      )
      sparkExecutor.SparkSQLResult()
    }

    //    Await.result(future, timeout second)
  }

}

class ExpQuery(data: String) {

  val rdd = new DataLoader().csv(data)

  def getSchema(q: String) = {
    val dml = q match {
      case "triangle"           => triangleDml
      case "chordalSquare"      => chordalSquareDml
      case "fourClique"         => fourCliqueDml
      case "fiveClique"         => fiveCliqueDml
      case "l31"                => l31Dml
      case "l32"                => l32Dml
      case "b313"               => b313Dml
      case "house"              => houseDml
      case "threeTriangle"      => threeTriangleDml
      case "solarSquare"        => solarSquareDml
      case "near5Clique"        => near5CliqueSchemaDml
      case "fiveCliqueMinusOne" => fiveCliqueMinusOneDml
      case _                    => throw new Exception(s"no such pattern:${q}")
    }

    assert(dml.endsWith(";"))

    ExpQueryHelper.dmlToSchemas(dml)
  }

  def getRelations(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(schema => Catalog.defaultCatalog().setContent(schema, rdd))
    schemas.map(schema => Relation(schema, rdd))
  }

  def getQuery(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(schema => Catalog.defaultCatalog().setContent(schema, rdd))
    val query0 =
      s"Join ${schemas.map(schema => s"${schema.name};").reduce(_ + _).dropRight(1)}"
    query0
  }

  //experiment query
  //triangle
  val triangleDml = "A-B;B-C;C-A;"

  //fourClique
  val fourCliqueDml = "A-B;B-C;C-D;D-A;A-C;B-D;"

  //  fiveClique
  val fiveCliqueDml = "A-B;B-C;C-D;D-E;E-A;A-C;A-D;B-D;B-E;C-E;"

  //  house
  val houseDml = "A-B;B-C;C-D;D-E;E-A;B-E;"

  //  threeTriangle
  val threeTriangleDml = "A-B;B-C;C-D;D-E;E-A;B-E;C-E;"

  //  near5Clique
  val near5CliqueSchemaDml = "A-B;B-C;C-D;D-E;E-A;B-E;B-D;C-E;"

  //  fiveCliqueMinusOne, A-D removed
  val fiveCliqueMinusOneDml = "A-B;B-C;C-D;D-E;E-A;A-C;B-D;B-E;C-E;"

  //optional query
  //  lolipop
  val l31Dml = "A-B;B-C;C-A;A-D;"
  val l32Dml = "A-B;B-C;C-A;A-D;D-E;"

  //  barbell
  val b313Dml = "A-B;B-C;C-A;C-D;D-E;D-F;E-F;"

  //  solarSquare
  val solarSquareDml = "A-B;B-C;C-D;D-A;A-E;B-E;C-E;D-E;"

  //  chordalSquare
  val chordalSquareDml = "A-B;B-C;A-C;A-D;C-D;"

}

object ExpQueryHelper {

  //we use a very simple dml like "A-B; A-C; A-D;".
  def dmlToSchemas(dml: String): Seq[RelationSchema] = {
    val catalog = Catalog.defaultCatalog()
    val pattern = "(([A-Z])-([A-Z]);)".r
    val schemas = pattern
      .findAllMatchIn(dml)
      .toArray
      .map { f =>
        val src = f.subgroups(1)
        val dst = f.subgroups(2)
        val id = catalog.nextRelationID()
        RelationSchema(s"R${id}", Seq(src, dst))
      }

    schemas.foreach { f =>
      f.register()
    }

    schemas
  }
}

class FutureTask[T](f: => Future[T]) extends TimerTask {
  val promise = Promise[T]()
  def run(): Unit = promise.completeWith(f)
  override def cancel() = {
    val result = super.cancel
    if (result) promise.complete(Failure(new CancellationException))
    result
  }
}

object FutureTask {
  implicit def toFuture[T](task: FutureTask[T]) = task.promise.future

  def scheduleFlat[T](
    when: Duration
  )(f: => Future[T])(implicit timer: Timer = defaultTimer): FutureTask[T] = {
    val task = new FutureTask(f)
    timer.schedule(task, when.toMillis)
    task
  }

  def schedule[T](when: Duration)(f: => T)(
    implicit timer: Timer = defaultTimer,
    ctx: ExecutionContext
  ): FutureTask[T] =
    scheduleFlat(when)(Future(f))(timer)

  val defaultTimer = new java.util.Timer(true)
}
