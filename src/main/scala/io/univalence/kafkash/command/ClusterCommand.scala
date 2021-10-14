package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, DescribeClusterOptions}
import org.jline.builtins.Completers.TreeCompleter
import org.jline.builtins.Completers.TreeCompleter.node

class ClusterCommand(admin: AdminClient) extends KafkaCliCommand {

  import scala.jdk.CollectionConverters._

  override val name: String = "cluster"
  override val completerNode: TreeCompleter.Node =
    node(name)

  override def recognize(commandLine: String): Boolean =
    commandLine == name

  override def run(commandLine: String): Unit = {
    val options =
      new DescribeClusterOptions()
        .timeoutMs((defaultTimeout.getSeconds * 1000).toInt)
    val cluster = admin.describeCluster(options)
    val clusterId = cluster.clusterId().get()
    val nodes = cluster.nodes().get().asScala

    Printer.print(Console.MAGENTA, s"ClusterId: $clusterId")
    nodes.foreach { node =>
      Printer.print(s"\tid:${node.id()} rack:${node.rack()} host:${node.host()} port:${node.port()}")
    }
  }
}
