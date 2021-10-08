package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions}
import org.jline.builtins.Completers.TreeCompleter

class TopicListCommand(admin: AdminClient) extends KafkaCliCommand {
  import scala.jdk.CollectionConverters._

  override val name: String = "topics"
  override val completerNode: TreeCompleter.Node = TreeCompleter.node(name)


  override def recognize(commandLine: String): Boolean =
    name == commandLine

  override def run(commandLine: String): Unit = {
    val options =
      new ListTopicsOptions()
        .timeoutMs((defaultTimeout.getSeconds * 1000).toInt)

    val topics: Seq[String] =
      admin
        .listTopics(options)
        .names()
        .get()
        .asScala
        .toSeq

    topics.foreach(t => Printer.print(Console.BLUE, t))
  }

}
