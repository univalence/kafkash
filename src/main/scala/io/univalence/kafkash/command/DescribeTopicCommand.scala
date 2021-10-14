package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, DescribeTopicsOptions, TopicDescription}
import org.jline.builtins.Completers.TreeCompleter
import org.jline.builtins.Completers.TreeCompleter.node
import org.jline.reader.impl.completer.StringsCompleter

class DescribeTopicCommand(admin: AdminClient, topics: => Seq[String])
    extends KafkaCliCommand {
  import scala.jdk.CollectionConverters._

  override val name: String = "topic"
  override val completerNode: TreeCompleter.Node =
    node(name, node(new StringsCompleter(topics.asJava)))

  override def recognize(commandLine: String): Boolean =
    name == commandLine.split("\\s+", 2)(0)

  override def run(commandLine: String): Unit = {
    val topicName = commandLine.split("\\s+", 2)(1)
    val options =
      new DescribeTopicsOptions()
        .timeoutMs((defaultTimeout.getSeconds * 1000).toInt)

    val topic: TopicDescription =
      admin
        .describeTopics(Seq(topicName).asJava, options)
        .values()
        .get(topicName)
        .get()

    Printer.print(
      Console.MAGENTA,
      s"Topic: ${topic.name()}"
    )
    topic.partitions().asScala.foreach { info =>
      val replicas = info.replicas().asScala.map(_.idString()).mkString(",")
      val isr      = info.isr().asScala.map(_.idString()).mkString(",")

      Printer.print(
        s"\tpartition: ${topic.name()}-${info
          .partition()}: leader: ${info.leader().idString()} replicas: $replicas isr: $isr"
      )
    }
  }

}
