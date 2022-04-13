package io.univalence.kafkash

import zio.{RIO, Task, UIO, ZIO}

trait ZWrapped[A](private val a: A) {

  final protected def executeTotal[B](f: A => B): UIO[B] = ZIO.succeed(f(a))

  final protected def executeTotalM[R, E, B](f: A => ZIO[R, E, B]): ZIO[R, E, B] = f(a)

  final protected def unsafeTotal[B](f: A => B): B = f(a)

  final def execute[B](f: A => B): Task[B] = Task.attempt(f(a))

  final def executeM[R, B](f: A => RIO[R, B]): RIO[R, B] = Task.attempt(f(a)).flatten

}
