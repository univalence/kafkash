package io.univalence.kafkash.command

import org.apache.kafka.clients.admin.{AdminClient, DescribeClusterOptions, DescribeFeaturesOptions, FeatureMetadata}
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

    val featuresOptions = new DescribeFeaturesOptions().timeoutMs((defaultTimeout.getSeconds * 1000).toInt)
    val features: FeatureMetadata =
      admin.describeFeatures(featuresOptions)
        .featureMetadata()
        .get()

    Printer.print(Console.MAGENTA, s"ClusterId: $clusterId")
    nodes.foreach { node =>
      Printer.print(s"\tid:${node.id()} rack:${node.rack()} host:${node.host()} port:${node.port()}")
    }
    if (!features.finalizedFeatures.isEmpty) {
      Printer.print("\tFinalized features:")
      features.finalizedFeatures().asScala.foreach { case (feature, versions) =>
        Printer.print(s"\t\t$feature: ${versions.minVersionLevel()} -> ${versions.maxVersionLevel()}")
      }
    }
    if (!features.supportedFeatures().isEmpty) {
      Printer.print("\tSupported features:")
      features.supportedFeatures().asScala.foreach { case (feature, versions) =>
        Printer.print(s"\t\t$feature: ${versions.minVersion()} -> ${versions.maxVersion()}")
      }
    }
  }
}
