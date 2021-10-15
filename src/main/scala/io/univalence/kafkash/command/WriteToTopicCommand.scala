package io.univalence.kafkash.command

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.jline.builtins.Completers.TreeCompleter._
import org.jline.reader.impl.completer.StringsCompleter

class WriteToTopicCommand(
    producer: KafkaProducer[String, String],
    topics:   => Seq[String]
) extends KafkaCliCommand {

  import scala.jdk.CollectionConverters._

  override val name: String        = "into"
  override val completerNode: Node = node(name, node(new StringsCompleter(topics.asJava), node("send")))

  override def recognize(commandLine: String): Boolean = commandLine.split("\\s+")(0) == name

  override def run(commandLine: String): Unit = {
    val args     = commandLine.split("\\s+", 4)
    val topic    = args(1)
    val dataArgs = args(3).trim

    if (dataArgs.startsWith("key:")) {
      val kv = dataArgs.split("\\s+", 2)(1).split("value:", 2)
      sendData(topic, Option(kv(0).trim), kv(1).trim, producer)
    } else {
      sendData(topic, None, dataArgs, producer)
    }
  }

  def sendData(
      topic: String,
      key: Option[String],
      value: String,
      producer: KafkaProducer[String, String]
  ): Unit = {
    val record =
      new ProducerRecord[String, String](
        topic,
        key.orElse(key).orNull,
        value
      )
    try {
      val result    = producer.send(record).get(defaultTimeout.getSeconds, TimeUnit.SECONDS)
      val timestamp = toLocalDateTime(result.timestamp())

      Printer.print(
        Console.YELLOW,
        s"${result.topic()}_${result.partition()}@${result.offset()}:$timestamp"
      )
    } catch {
      case e: Exception => println(s"Error: ${e.getMessage}")
    }
  }

}
