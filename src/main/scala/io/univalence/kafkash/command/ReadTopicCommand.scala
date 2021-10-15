package io.univalence.kafkash.command

import java.nio.charset.StandardCharsets
import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.jline.builtins.Completers.TreeCompleter
import org.jline.builtins.Completers.TreeCompleter.node
import org.jline.reader.impl.completer.StringsCompleter

class ReadTopicCommand(
    consumer: KafkaConsumer[String, String],
    topics:   => Seq[String]
) extends KafkaCliCommand {

  import scala.jdk.CollectionConverters._

  val pollTimeout = 999_999_999L

  override val name: String = "from"
  override val completerNode: TreeCompleter.Node =
    node(
      name,
      node(new StringsCompleter(topics.asJava), node("last")),
      node(new StringsCompleter(topics.asJava), node("follow")),
//      node(new StringsCompleter(topics.asJava), node("partition:")),
      node(new StringsCompleter(topics.asJava))
    )

  override def recognize(commandLine: String): Boolean = commandLine.split("\\s+", 2)(0) == name

  override def run(commandLine: String): Unit = {
    val args  = commandLine.split("\\s+")
    val topic = args(1)

    args.lift(2) match {
      case Some("last")   => readFrom(topic, consumer, args(3).toInt)
      case Some("follow") => followTopic(topic, consumer)
      case None    => readFrom(topic, consumer, 1)
      case Some(_) => Printer.print(Console.RED, s"unknown command: $commandLine")
    }
  }

  def readFrom(
      topic: String,
      consumer: KafkaConsumer[String, String],
      count: Int
  ): Unit = {
    try {
      val partitions: List[TopicPartition] = getPartitions(topic, consumer)

      val beginOffsets: Map[TopicPartition, Long] =
        consumer
          .beginningOffsets(partitions.asJava)
          .asScala
          .map { case (p, o) => p -> o.longValue() }
          .toMap

      val endOffsets: Map[TopicPartition, Long] =
        consumer
          .endOffsets(partitions.asJava)
          .asScala
          .map { case (p, o) => p -> o.longValue() }
          .toMap

      val deltaOffsets: Map[TopicPartition, Long] =
        partitions
          .map(partition =>
            partition -> (
              endOffsets(partition) - beginOffsets(partition)
            )
          )
          .toMap

      if (deltaOffsets.forall(_._2 == 0)) {
        Printer.print(Console.RED, "No data available.")
      } else {
        consumer.assign(partitions.asJava)
        endOffsets.foreach { case (partition, endOffset) =>
          val offset = Math.max(endOffset - count, beginOffsets(partition))

          consumer.seek(partition, offset)
        }

        var records = consumer.poll(java.time.Duration.ofSeconds(5))
        var i       = 0
        if (records.isEmpty) {
          Printer.print(Console.RED, "No data available.")
        } else {
          while (!records.isEmpty && i < count) {
            records.iterator().asScala.foreach(printRecord)
            records = consumer.poll(defaultTimeout.dividedBy(2L))
            i += 1
          }
        }
      }
    } finally {
      consumer.unsubscribe()
    }
  }

  def followTopic(
      topic: String,
      consumer: KafkaConsumer[String, String]
  ): Unit = {
    Printer.print(Console.RED, "(hit Ctrl+C to stop...)")
    consumer.assign(getPartitions(topic, consumer).asJava)
    try {
      while (true) {
        val records = consumer.poll(Duration.ofDays(pollTimeout))
        records.asScala.foreach(printRecord)
      }
    } catch {
      case _: InterruptedException => ()
    } finally {
      consumer.unsubscribe()
    }
  }

  private def getPartitions(
      topic: String,
      consumer: KafkaConsumer[String, String]
  ): List[TopicPartition] =
    consumer
      .partitionsFor(topic)
      .asScala
      .toList
      .map(partitionInfo => new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))

  def printRecord(record: ConsumerRecord[String, String]): Unit = {
    Printer.print(
      Console.MAGENTA,
      s"${Console.UNDERLINED}${record.topic()}-${record.partition()}${Console.RESET}${Console.MAGENTA}@${record
        .offset()}:${toLocalDateTime(record.timestamp())}"
    )

    val headers =
      record
        .headers()
        .asScala
        .map(h => s"${h.key()}: ${new String(h.value(), StandardCharsets.UTF_8)}")
        .mkString(", ")

    Printer.print(Console.GREEN, s"\tHeaders: $headers")

    Printer.print(Console.YELLOW, s"\tKey: ${record.key()}")
    Printer.print(Console.BLUE, s"\tValue:")
    Printer.print(Console.BLUE, record.value())
  }

}
