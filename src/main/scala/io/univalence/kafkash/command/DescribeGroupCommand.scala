package io.univalence.kafkash.command

import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.jline.builtins.Completers.TreeCompleter
import org.jline.builtins.Completers.TreeCompleter.node
import org.jline.reader.impl.completer.StringsCompleter

class DescribeGroupCommand(admin: AdminClient, consumer: KafkaConsumer[String, String], groups: => Seq[String])
    extends KafkaCliCommand {
  import scala.jdk.CollectionConverters._

  override val name: String                      = "group"
  override val completerNode: TreeCompleter.Node = node(name, node(new StringsCompleter(groups.asJava)))

  override def recognize(commandLine: String): Boolean = name == commandLine.split("\\s+", 2)(0)

  override def run(commandLine: String): Unit = {
    val groupName = commandLine.split("\\s+", 2)(1)
    val options =
      new DescribeConsumerGroupsOptions()
        .timeoutMs((defaultTimeout.getSeconds * 1000).toInt)

    val group: ConsumerGroupDescription =
      admin
        .describeConsumerGroups(Seq(groupName).asJava, options)
        .describedGroups()
        .get(groupName)
        .get()

    val listOptions = new ListConsumerGroupOffsetsOptions().timeoutMs((defaultTimeout.getSeconds * 1000).toInt)
    val offsets: Map[TopicPartition, OffsetAndMetadata] =
      admin
        .listConsumerGroupOffsets(group.groupId(), listOptions)
        .partitionsToOffsetAndMetadata()
        .get()
        .asScala
        .toMap

    Printer.print(
      Console.MAGENTA,
      s"Group: ${group.groupId()} state:${group.state().name()} coordinator:${group.coordinator().idString()} assignator:${group.partitionAssignor()}"
    )

    group
      .members()
      .asScala
      .foreach { member =>
        Printer.print(
          Console.GREEN,
          s"\tclientId:${member.clientId()}\n\tconsumerId:${member.consumerId()}\n\thost:${member.host()}"
        )
        val partitions   = member.assignment().topicPartitions().asScala
        val beginOffsets = consumer.beginningOffsets(partitions.asJava).asScala.toMap
        val endOffsets   = consumer.endOffsets(partitions.asJava).asScala.toMap

        partitions
          .foreach { partition =>
            val offset =
              offsets
                .get(partition)
                .map(o => s" offset:${o.offset()}")
                .getOrElse("")

            Printer.print(
              s"\t\t${Console.YELLOW}${partition.topic()}-${partition
                .partition()}:${Console.RESET} range:${beginOffsets(partition)} -> ${endOffsets(partition)}$offset"
            )
          }
      }
  }

}
