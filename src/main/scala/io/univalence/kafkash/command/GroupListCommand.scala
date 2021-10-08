package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, ConsumerGroupListing, ListConsumerGroupsOptions}
import org.jline.builtins.Completers.TreeCompleter

class GroupListCommand(admin: AdminClient) extends KafkaCliCommand {
  import scala.jdk.CollectionConverters._

  override val name: String = "groups"
  override val completerNode: TreeCompleter.Node = TreeCompleter.node(name)

  override def recognize(commandLine: String): Boolean =
    name == commandLine

  override def run(commandLine: String): Unit = {
    val options =
      new ListConsumerGroupsOptions()
        .timeoutMs((defaultTimeout.getSeconds * 1000).toInt)

    val groups: Seq[ConsumerGroupListing] =
      admin
        .listConsumerGroups(options)
        .valid()
        .get()
        .asScala
        .toSeq

    groups.foreach(t => Printer.print(Console.BLUE, t.groupId()))
  }

}
