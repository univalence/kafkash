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
      topics: => Seq[String]
  ): Seq[KafkaCliCommand] =
    Seq(
      new TopicListCommand(admin),
      new GroupListCommand(admin),
      new DescribeTopicCommand(admin, topics),
      new ReadTopicCommand(consumer, topics),
      new WriteToTopicCommand(producer, topics),
      new NewCommand(admin),
      new DeleteCommand(admin, topics)
    )

  def main(args: Array[String]): Unit = {
    val uuid = UUID.randomUUID()

    val admin =
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> defaultKafkaServers,
          AdminClientConfig.CLIENT_ID_CONFIG         -> s"admin-cli-client-$uuid"
        ).asJava
      )

    val producer =
      new KafkaProducer[String, String](
        Map[String, AnyRef](
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> defaultKafkaServers,
          ProducerConfig.CLIENT_ID_CONFIG         -> s"producer-cli-$uuid"
        ).asJava,
        Serdes.String().serializer(),
        Serdes.String().serializer()
      )

    val consumer =
      new KafkaConsumer[String, String](
        Map[String, AnyRef](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> defaultKafkaServers,
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
          val commands  = commandsFrom(admin, consumer, producer, topics)
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
