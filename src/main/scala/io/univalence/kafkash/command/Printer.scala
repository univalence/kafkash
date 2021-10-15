package io.univalence.kafkash.command

object Printer {
  def print(x: Any): Unit                = Console.println(x)
  def print(color: String, x: Any): Unit = Console.println(color + x.toString + Console.RESET)
}
