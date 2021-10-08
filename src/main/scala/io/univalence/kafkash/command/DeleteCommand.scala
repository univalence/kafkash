package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, DeleteTopicsOptions}
import org.jline.builtins.Completers.TreeCompleter
import org.jline.builtins.Completers.TreeCompleter.node
import org.jline.reader.impl.completer.StringsCompleter

class DeleteCommand(admin: AdminClient, topics: => Seq[String])
    extends KafkaCliCommand {

  import scala.jdk.CollectionConverters._

  override val name: String = "delete"
  override val completerNode: TreeCompleter.Node =
    node(name, node("topic", node(new StringsCompleter(topics.asJava))))

  override def recognize(commandLine: String): Boolean =
    commandLine.split("\\s+", 2)(0) == name

  override def run(commandLine: String): Unit = {
    val args  = commandLine.split("\\s+")
    val topic = args(2)

    deleteTopic(topic)
  }

  def deleteTopic(topic: String): Unit = {
    val options =
      new DeleteTopicsOptions()
        .timeoutMs(defaultTimeout.toMillis.toInt)

    admin.deleteTopics(List(topic).asJava, options).all().get()
  }
}
