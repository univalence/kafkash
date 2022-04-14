package io.univalence.kafkash.utils

import sun.misc.{Signal, SignalHandler}

import zio.*

import scala.concurrent.Future

object CtrlCSignalHandler {

  /**
   * Execute process until &lt;Ctrl+C> has been hit.
   *
   * Example of use:
   *
   * {{{
   *   CtrlCSignalHandler.doUntilCtrlC(ctrlCHit =>
   *     Console
   *       .printLine(s"hello")
   *       .schedule(
   *         Schedule.spaced(Duration.fromMillis(100))
   *           >>> Schedule.recurUntilZIO(_ => ctrlCHit.isDone)
   *       )
   * }}}
   *
   * @param process
   *   a function that take a promised synchronized on &lt;Ctrl+C> and
   *   execute a process.
   * @return
   *   returns when &lt;Ctrl+C> has been hit and the process has been
   *   synchronized on the promised.
   */
  def doUntilCtrlC[R, E, A](
      process: Promise[Throwable, Unit] => ZIO[R, E, A]
  ): ZIO[R, E, Unit] =
    for {
      p <- Promise.make[Throwable, Unit]
      _ <- p.complete(CtrlCSignalHandler.readCtrlC).fork
      _ <- process(p)
    } yield ()

  /**
   * Wait for &lt;Ctrl+C> to be hit.
   *
   * @return
   *   returns once &lt;Ctrl+C> has been hit.
   */
  def readCtrlC: ZIO[Any, Throwable, Unit] =
    (for {
      handler <- ZIO.service[CtrlCSignalHandler]
      _       <- ZIO.fromFuture(_ => handler.sync)
    } yield ()).provideLayer(ctrlCHandlerLayer)

  private val SIGINT = new Signal("INT")

  private def ctrlCHandlerLayer: ZLayer[Any, Throwable, CtrlCSignalHandler] =
    ZLayer.scoped {
      val newHandler = new CtrlCSignalHandler

      ZIO
        .acquireRelease(Task.attempt {
          val oldHandler = Signal.handle(SIGINT, newHandler)
          if (oldHandler == SignalHandler.SIG_IGN) {
            throw new NoSuchElementException(
              s"cannot change handler for ${SIGINT.getName}(${SIGINT.getNumber})"
            )
          }
          oldHandler
        }) { oldHandler =>
          Task
            .attempt(Signal.handle(SIGINT, oldHandler))
            .foldZIO(
              e => UIO.succeed(e.printStackTrace()),
              ZIO.succeed
            )
        }
        .as(newHandler)
    }

}

private class CtrlCSignalHandler extends SignalHandler {
  val p: concurrent.Promise[Unit] = scala.concurrent.Promise()

  lazy val sync: Future[Unit] = p.future

  override def handle(sig: Signal): Unit =
    if (sig.getNumber == CtrlCSignalHandler.SIGINT.getNumber) {
      p.success(())
    } else ()
}
