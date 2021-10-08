package io.univalence.kafkash.command

import org.jline.builtins.Completers.TreeCompleter

trait KafkaCliCommand {
  val name: String
  val completerNode: TreeCompleter.Node
  def recognize(commandLine: String): Boolean
  def run(commandLine: String): Unit
}
