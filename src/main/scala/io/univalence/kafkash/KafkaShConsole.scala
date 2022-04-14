package io.univalence.kafkash

import org.jline.terminal.Terminal

import io.univalence.kafkash.wrapper.ZLineReader

import zio.*

object KafkaShConsole {

  object Color {
    val prompt: String    = scala.Console.MAGENTA
    val response: String  = scala.Console.YELLOW
    val error: String     = scala.Console.RED
    val important: String = scala.Console.MAGENTA
    val reset: String     = scala.Console.RESET
  }

  /** Kafka Sh console interface. */
  trait Console {
    def error(message: => Any): Task[Unit]

    def important(message: => Any): Task[Unit]

    def response(message: => Any): Task[Unit]

    def print(message: => Any): Task[Unit]

    def read(prompt: String): Task[String]
  }

  object Console extends Accessible[Console]

  /**
   * Kafka Sh console implementation.
   *
   * @param console
   *   ZIO console instance
   * @param terminal
   *   JLine terminal
   */
  case class ConsoleLive(console: zio.Console, terminal: Terminal) extends Console {

    val lineReader = new ZLineReader(terminal)

    override def error(message: => Any): Task[Unit] = console.printLine(s"${Color.error}$message${Color.reset}")

    override def important(message: => Any): Task[Unit] = console.printLine(s"${Color.important}$message${Color.reset}")

    override def response(message: => Any): Task[Unit] = console.printLine(s"${Color.response}$message${Color.reset}")

    override def print(message: => Any): Task[Unit] = console.printLine(s"$message")

    override def read(prompt: String): Task[String] =
      lineReader
        .read(s"${Color.prompt}$prompt${Color.reset} ")
        .map(_.trim)
  }

  val layer: URLayer[zio.Console with Terminal, Console] =
    ZLayer.fromFunction[zio.Console, Terminal, Console](ConsoleLive.apply)

}
