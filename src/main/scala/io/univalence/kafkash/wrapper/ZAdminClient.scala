package io.univalence.kafkash.wrapper

import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.errors.GroupIdNotFoundException

import zio.*

import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Try

class ZAdminClient(admin: AdminClient) extends ZWrapped(admin) {

  def listTopics: Task[Iterable[TopicListing]] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.listTopics().listings())
        .map(_.asScala)
    }

  def listGroups: Task[Iterable[ConsumerGroupListing]] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.listConsumerGroups().all())
        .map(_.asScala)
    }

  def createGroup(group: String, topic: String): Task[Unit] =
    executeM { admin =>

      val partitions: Task[Iterable[TopicPartition]] =
        ZIO
          .fromFutureJava(admin.describeTopics(List(topic).asJava).all())
          .map(
            _.asScala.values
              .flatMap(_.partitions().asScala.toList)
              .map(tp => new TopicPartition(topic, tp.partition()))
          )

      def offsetsOf(topicParitions: Iterable[TopicPartition]): Task[Map[TopicPartition, OffsetAndMetadata]] = {
        val offsets: Iterable[Task[List[(TopicPartition, OffsetAndMetadata)]]] =
          topicParitions.map { tp =>
            ZIO
              .fromFutureJava(admin.listOffsets(Map(tp -> OffsetSpec.latest()).asJava).all())
              .map(_.asScala.toMap.map { case (tp, offset) => tp -> new OffsetAndMetadata(offset.offset()) }.toList)
          }

        ZIO
          .collectAll(offsets)
          .map(_.flatten.toMap)
      }

      for {
        topicParitions <- partitions
        offsets        <- offsetsOf(topicParitions)
        _ <-
          ZIO
            .fromFutureJava(admin.alterConsumerGroupOffsets(group, offsets.asJava).all())
            .unit
      } yield ()
    }

  def createTopic(topic: String, partitionCount: Int, replicationCount: Int): Task[Unit] =
    executeM { admin =>
      val newTopic = new NewTopic(topic, partitionCount, replicationCount.toShort)

      ZIO.fromFutureJava(admin.createTopics(List(newTopic).asJava).all()).unit
    }

  def describeTopic(topic: String): Task[TopicDescription] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.describeTopics(List(topic).asJava).all())
        .foldZIO(
          {
            case e: org.apache.kafka.common.errors.UnknownTopicOrPartitionException =>
              Task.fail(new NoSuchElementException(topic, e))
            case e =>
              println(e)
              Task.fail(e)
          },
          descriptionMap => Task.succeed(descriptionMap.asScala.toMap.apply(topic))
        )
    }

  def describeGroup(group: String): Task[ConsumerGroupDescription] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.describeConsumerGroups(List(group).asJava).all())
        .map(_.asScala.toMap.apply(group))
    }

  def deleteTopic(topic: String): Task[Unit] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.deleteTopics(List(topic).asJava).all())
        .unit
    }

  def deleteGroup(group: String): Task[Unit] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.deleteConsumerGroups(List(group).asJava).all())
        .unit
    }

  def describeClusterId: Task[String] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.describeCluster().clusterId())
    }

  def describeClusterController: Task[Node] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.describeCluster().controller())
    }

  def describeClusterNodes: Task[List[Node]] =
    executeM { admin =>
      ZIO
        .fromFutureJava(admin.describeCluster().nodes())
        .map(_.asScala.toList)
    }

  def close: Task[Unit] =
    execute { admin =>
      admin.close(java.time.Duration.ofSeconds(1))
    }

  def toManaged: ZIO[Scope, Throwable, ZAdminClient] =
    ZIO.acquireRelease(UIO.succeed(this))(
      _.close.fold(
        _.printStackTrace(),
        identity
      )
    )

}

object ZAdminClient {
  def create(adminSettings: Map[String, AnyRef]): UIO[ZAdminClient] =
    for admin <- Task.succeed(AdminClient.create(adminSettings.asJava)) yield new ZAdminClient(admin)
}
