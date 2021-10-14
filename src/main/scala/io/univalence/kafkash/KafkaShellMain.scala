package io.univalence.kafkash

import io.univalence.kafkash.command._
import java.util.UUID
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.Serdes
import org.jline.builtins.Completers.TreeCompleter
import org.jline.reader.impl.history.DefaultHistory
import org.jline.reader.{
  EndOfFileException,
  LineReaderBuilder,
  UserInterruptException
}
import org.jline.terminal.TerminalBuilder

object KafkaShellMain {
  import scala.jdk.CollectionConverters._

  val defaultKafkaServers = "localhost:9092"

  def commandsFrom(
      admin: AdminClient,
      consumer: KafkaConsumer[String, String],
      producer: KafkaProducer[String, String],
      topics: => Seq[String],
      groups: => Seq[String]
  ): Seq[KafkaCliCommand] =
    Seq(
      new TopicListCommand(admin),
      new GroupListCommand(admin),
      new DescribeTopicCommand(admin, topics),
      new ReadTopicCommand(consumer, topics),
      new WriteToTopicCommand(producer, topics),
      new NewCommand(admin),
      new DeleteCommand(admin, topics),
      new ClusterCommand(admin),
      new DescribeGroupCommand(admin, consumer, groups)
    )

  def main(args: Array[String]): Unit = {
    val (parameters, _) = getParameters(args)

    if (parameters.contains("help")) {
      help()
      sys.exit(0)
    }

    val bootstrapServers =
      parameters
        .find(_._1 == "bootstrap.servers")
        .map(_._2.mkString(","))
        .getOrElse(defaultKafkaServers)

    val uuid = UUID.randomUUID()
    Printer.print(Console.YELLOW, s"connecting to $bootstrapServers...")
    val admin =
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
          AdminClientConfig.CLIENT_ID_CONFIG         -> s"admin-cli-client-$uuid"
        ).asJava
      )
    val cluster = new ClusterCommand(admin)
    cluster.run(cluster.name)

    val producer =
      new KafkaProducer[String, String](
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
          ProducerConfig.CLIENT_ID_CONFIG         -> s"producer-cli-$uuid"
        ).asJava,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )

    val consumer =
      new KafkaConsumer[String, String](
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
          ConsumerConfig.CLIENT_ID_CONFIG         -> s"consumer-cli-$uuid",
          ConsumerConfig.GROUP_ID_CONFIG          -> s"consumer-grp-$uuid"
        ).asJava,
        Serdes.String().deserializer(),
        Serdes.String().deserializer()
      )

    val terminal =
      TerminalBuilder
        .builder()
        .name("Kafka CLI")
        .system(true)
        .build()

    val history = new DefaultHistory()

    try {
      var topics = getTopics(admin)
      var groups = getGroups(admin)

      var done = false
      while (!done) {
        try {
          val commands  = commandsFrom(admin, consumer, producer, topics, groups)
          val completer = treeCompleterFrom(commands)
          val reader =
            LineReaderBuilder
              .builder()
              .terminal(terminal)
              .history(history)
              .completer(completer)
              .build()

          val commandLine =
            reader.readLine(s"${Console.MAGENTA}>${Console.RESET} ").trim
          val maybeCommand: Option[KafkaCliCommand] =
            commands.find(_.recognize(commandLine))
          maybeCommand
            .map { command =>
              command.run(commandLine)

              topics = getTopics(admin)
              groups = getGroups(admin)
            }
            .getOrElse {
              if (commandLine == "quit") {
                done = true
              } else {
                Printer.print(Console.RED, "unknown command in: " + commandLine)
              }
            }
        } catch {
          case _: EndOfFileException     => done = true
          case _: UserInterruptException => done = true
        }
      }

    } finally {
      admin.close()
      consumer.close()
      producer.close()
      terminal.close()
    }
  }

  def help(): Unit = {
    Printer.print(
      s"""kafkash: shell for Apache Kafka cluster
usage kafkash [--bootstrap.servers <server_list>] [--help]
    --bootstrap.servers <server_list>        The servers to connect to, in the
                                             form HOST1:PORT1[,HOST2:PORT2]*.
                                             Default: $defaultKafkaServers.
    --help                                   Print this help.
"""
    )
  }

  def getParameters(
      args: Array[String]
  ): (Map[String, List[String]], List[String]) = {
    val (_parameters, values, current) =
      args.toList
        .foldLeft(
          (
            List.empty[(String, String)],
            List.empty[String],
            Option.empty[String]
          )
        ) {
          case ((parameters, values, None), value) =>
            if (value.startsWith("--")) {
              if (value.contains("=")) {
                val kv = value.substring(2).split("=", 2)
                (parameters :+ (kv(0) -> kv(1)), values, None)
              } else {
                (parameters, values, Some(value.substring(2)))
              }
            } else {
              (parameters, values :+ value, None)
            }
          case ((parameters, values, Some(current)), value) =>
            if (value.startsWith("--")) {
              if (value.contains("=")) {
                val kv = value.substring(2).split("=", 2)
                (
                  parameters :+ (current -> "") :+ (kv(0) -> kv(1)),
                  values,
                  None
                )
              } else {
                (
                  parameters :+ (current -> ""),
                  values,
                  Some(value.substring(2))
                )
              }
            } else {
              (parameters :+ (current -> value), values, None)
            }
        }

    val parameters =
      (_parameters ++ current.map(_ -> ""))
        .groupBy(_._1)
        .map { case (k, l) => k -> l.map(_._2).filterNot(_.isEmpty) }

    (parameters, values)
  }

  def treeCompleterFrom(
      commands: Seq[KafkaCliCommand]
  ): TreeCompleter =
    new TreeCompleter(commands.map(_.completerNode).asJava)

  def getGroups(admin: AdminClient): List[String] =
    admin
      .listConsumerGroups()
      .all()
      .get()
      .asScala
      .map(_.groupId())
      .toList

  def getTopics(admin: AdminClient): List[String] =
    admin
      .listTopics()
      .listings()
      .get()
      .asScala
      .filterNot(_.isInternal)
      .map(_.name())
      .toList

}
