package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsOptions, NewTopic}
import org.jline.builtins.Completers.TreeCompleter
import org.jline.builtins.Completers.TreeCompleter.node

class NewCommand(admin: AdminClient) extends KafkaCliCommand {

  import scala.jdk.CollectionConverters._

  val defaultPartitions: Int = 3
  val defaultReplicas: Short = 1

  override val name: String                      = "new"
  override val completerNode: TreeCompleter.Node = node(name, node("topic"))

  override def recognize(commandLine: String): Boolean = commandLine.split("\\s+", 2)(0) == name

  override def run(commandLine: String): Unit = {
    val args  = commandLine.split("\\s+")
    val topic = args(2)

    // new topic <name> partitions: <n> replicas: <n>
    val partitionCount =
      if (args.length >= 5 && args(3) == "partitions:") args(4).toInt
      else defaultPartitions

    val replicaCount =
      if (args.length >= 7 && args(5) == "replicas:") args(6).toShort
      else defaultReplicas

    createTopic(topic, partitionCount, replicaCount)
  }

  def createTopic(
      topic: String,
      partitionCount: Int,
      replicaCount: Short
  ): Unit = {
    val options =
      new CreateTopicsOptions()
        .timeoutMs(defaultTimeout.toMillis.toInt)

    val newTopic = new NewTopic(topic, partitionCount, replicaCount)

    admin.createTopics(List(newTopic).asJava, options).all().get()
  }
}
