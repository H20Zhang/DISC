package org.apache.spark.adj.utils.exp

import java.util.{Timer, TimerTask}
import java.util.concurrent.{CancellationException, FutureTask}

import org.apache.spark.adj.database.{Query, RelationSchema}
import org.apache.spark.adj.execution.misc.DataLoader
import org.apache.spark.adj.utils.misc.SparkSingle

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Failure

class ExpExecutor(data: String,
                  query: String,
                  timeout: Int,
                  isCommOnly: Boolean) {

  def execute() = {
    val expQuery = new ExpQuery(data)

    import scala.concurrent.ExecutionContext.Implicits.global

    import scala.concurrent.duration._
    val someTask = FutureTask.schedule(timeout seconds) {
      SparkSingle.getSparkContext().cancelAllJobs()
      println("timeout")
//      System.exit(0)
    }

    SparkSingle.appName =
      s"ADJ-data:${data}-query:${query}-timeout:${timeout}-isCommOnly:${isCommOnly}"

//    val future = Future {
    if (isCommOnly) {
      Query.commOnlyQuery(expQuery.query(query))
    } else {
      Query.countQuery(expQuery.query(query))
    }

//    Await.result(future, timeout second)
  }

}

class ExpQuery(data: String) {

  val rdd = new DataLoader().csv(data)

  def getSchema(q: String) = q match {
    case "triangle"      => triangleSchema()
    case "chordalSquare" => chordalSquareSchema()
    case "fourClique"    => fourCliqueSchema()
    case "l31"           => l31Schema()
    case "b313"          => b313Schema()
    case "house"         => houseSchema()
    case "near5Clique"   => near5CliqueSchema()
    case _               => throw new Exception(s"no such pattern:${q}")
  }

  def query(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(_.register(rdd))
    val query0 =
      s"Join ${schemas.map(schema => s"${schema.name};").reduce(_ + _).dropRight(1)}"
    query0
  }

  def triangleSchema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))

    val schemas = Seq(schemaR0, schemaR1, schemaR2)
    schemas
  }

  def fourCliqueSchema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("A", "C"))
    val schemaR3 = RelationSchema("R3", Seq("A", "D"))
    val schemaR4 = RelationSchema("R4", Seq("C", "D"))
    val schemaR5 = RelationSchema("R5", Seq("B", "D"))

    val schemas =
      Seq(schemaR0, schemaR1, schemaR2, schemaR3, schemaR4, schemaR5)
    schemas
  }

  def chordalSquareSchema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("A", "C"))
    val schemaR3 = RelationSchema("R3", Seq("A", "D"))
    val schemaR4 = RelationSchema("R4", Seq("C", "D"))

    val schemas =
      Seq(schemaR0, schemaR1, schemaR2, schemaR3, schemaR4)
    schemas
  }

//  lolipop
  def l31Schema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))
    val schemaR3 = RelationSchema("R3", Seq("A", "D"))

    val schemas = Seq(schemaR0, schemaR1, schemaR2, schemaR3)
    schemas
  }

//  barbell
  def b313Schema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))
    val schemaR3 = RelationSchema("R3", Seq("C", "D"))
    val schemaR4 = RelationSchema("R4", Seq("D", "E"))
    val schemaR5 = RelationSchema("R5", Seq("D", "F"))
    val schemaR6 = RelationSchema("R6", Seq("E", "F"))

    val schemas =
      Seq(schemaR0, schemaR1, schemaR2, schemaR3, schemaR4, schemaR5, schemaR6)

    schemas
  }

  //  house
  def houseSchema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("C", "A"))
    val schemaR3 = RelationSchema("R3", Seq("B", "D"))
    val schemaR4 = RelationSchema("R4", Seq("C", "E"))
    val schemaR5 = RelationSchema("R5", Seq("D", "E"))

    val schemas =
      Seq(schemaR0, schemaR1, schemaR2, schemaR3, schemaR4, schemaR5)

    schemas
  }

  //  near5Clique
  def near5CliqueSchema() = {
    val schemaR0 = RelationSchema("R0", Seq("A", "B"))
    val schemaR1 = RelationSchema("R1", Seq("B", "C"))
    val schemaR2 = RelationSchema("R2", Seq("A", "C"))
    val schemaR3 = RelationSchema("R3", Seq("A", "D"))
    val schemaR4 = RelationSchema("R4", Seq("C", "D"))
    val schemaR5 = RelationSchema("R5", Seq("B", "D"))
    val schemaR6 = RelationSchema("R6", Seq("A", "F"))
    val schemaR7 = RelationSchema("R7", Seq("D", "F"))

    val schemas = Seq(
      schemaR0,
      schemaR1,
      schemaR2,
      schemaR3,
      schemaR4,
      schemaR5,
      schemaR6,
      schemaR7
    )

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
