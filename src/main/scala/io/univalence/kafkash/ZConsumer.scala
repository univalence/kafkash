package io.univalence.kafkash

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import zio.*

class ZConsumer[K, V](consumer: KafkaConsumer[K, V]) extends ZWrapped[KafkaConsumer[K, V]](consumer) {

  import scala.jdk.CollectionConverters.*

  def assign(topicPartitions: List[TopicPartition]): Task[Unit] = execute(_.assign(topicPartitions.asJava))

  def assign(topic: String): Task[Unit] =
    executeM { consumer =>
      val partitions: Option[List[TopicPartition]] =
        consumer
          .listTopics()
          .asScala
          .get(topic)
          .map(_.asScala.toList.map(pi => new TopicPartition(pi.topic(), pi.partition())))

      partitions.map(assign).getOrElse(Task.fail(new IllegalArgumentException(s"Topic unknown: $topic")))
    }

  def unsubscribe(): Task[Unit] = execute(_.unsubscribe())

  def seekToEnd(topicPartitions: List[TopicPartition]): Task[Unit] = execute(_.seekToEnd(topicPartitions.asJava))

  def seek(topicPartitions: List[TopicPartition], offsets: List[Long]): Task[Unit] =
    execute { consumer =>
      topicPartitions.zip(offsets).map { case (tp, o) => consumer.seek(tp, o) }
    }

  def position(topicPartitions: List[TopicPartition]): Task[List[Long]] =
    execute { consumer =>
      topicPartitions.map(tp => consumer.position(tp))
    }

  def poll(timeout: Duration): Task[ConsumerRecords[K, V]] = execute(_.poll(timeout))

}
