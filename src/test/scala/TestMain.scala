import org.apache.kafka.clients.admin.{
  AdminClient,
  AdminClientConfig,
  AlterConsumerGroupOffsetsOptions,
  CreateTopicsOptions,
  CreateTopicsResult,
  NewTopic
}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import scala.util.Using

object TestMain {

  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit =
    Using(new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))) { kafka =>
      kafka.start()
      val bootstrap = kafka.getBootstrapServers

      val admin =
        AdminClient.create(
          Map[String, AnyRef](
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap
          ).asJava
        )

      val result: CreateTopicsResult =
        admin.createTopics(
          List(
            new NewTopic("in", 3, 1.toShort),
            new NewTopic("out", 3, 1.toShort)
          ).asJava
        )

      result.all().get()

      admin
        .alterConsumerGroupOffsets(
          "group-1",
          Map[TopicPartition, OffsetAndMetadata](
            new TopicPartition("in", 0) -> new OffsetAndMetadata(0L),
            new TopicPartition("in", 1) -> new OffsetAndMetadata(0L),
            new TopicPartition("in", 2) -> new OffsetAndMetadata(0L)
          ).asJava
        )
        .all()
        .get()

      admin
        .listConsumerGroups()
        .all()
        .get()
        .asScala
        .foreach { group =>
          println(s"${group.groupId()} ${group.state().map(_.name()).orElse("???")}")
          val groupDescription =
            admin.describeConsumerGroups(List(group.groupId()).asJava).all().get().asScala(group.groupId())
          val members = groupDescription.members().asScala
          println(s"Members (${members.size}):")
          members.foreach { member =>
            val assigments = member.assignment().topicPartitions().asScala
            println(s"\t${member.host()} (${assigments.size}): ${assigments.mkString(", ")}")
          }
        }
    }.get
}
