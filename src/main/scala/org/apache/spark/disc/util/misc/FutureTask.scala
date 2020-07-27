package org.apache.spark.disc.util.misc

import java.util.concurrent.CancellationException
import java.util.{Timer, TimerTask}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

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
