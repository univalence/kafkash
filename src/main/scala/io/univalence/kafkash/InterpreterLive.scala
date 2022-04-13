package io.univalence.kafkash

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{GroupIdNotFoundException, TopicExistsException, UnknownTopicOrPartitionException}

import io.univalence.kafkash.KafkaConnection.Connection
import io.univalence.kafkash.KafkaInterpreter.Interpreter
import io.univalence.kafkash.KafkaShApp.RunningState
import io.univalence.kafkash.KafkaShConsole.Console

import zio.*

import scala.collection.mutable

import java.awt.event.KeyEvent
import java.nio.charset.StandardCharsets

case class InterpreterLive(connection: Connection, console: Console) extends Interpreter {
  import scala.jdk.CollectionConverters.*

  override def showTopic(topic: String): Task[RunningState] =
    (
      for {
        admin       <- connection.admin
        description <- admin.describeTopic(topic)
        partititonCount = description.partitions().size()
        _ <-
          console.response(
            s"Topic: ${description.name()} "
              + s"(id:${description.topicId()} - partitions: $partititonCount - internal: ${description.isInternal})"
          )
        _ <-
          ZIO.foreach {
            description
              .partitions()
              .asScala
          } { partition =>
            console.response(
              s"\tpartition ${partition.partition()} "
                + s"- replicas:${partition.replicas().asScala.map(_.id()).mkString(",")} "
                + s"- leader:${partition.leader().id()} "
                + s"- isr:${partition.isr().asScala.map(_.id()).mkString(",")} "
            )
          }
      } yield RunningState.Continue
    ).foldZIO(
      {
        case e: NoSuchElementException => console.error(s"Unknown topic: $topic") *> RunningState.ContinueM
        case e =>
          e.printStackTrace()
          RunningState.StopM
      },
      UIO.succeed
    )

  override def showTopics: Task[RunningState] =
    for {
      admin  <- connection.admin
      topics <- admin.listTopics
      _      <- ZIO.foreach(topics.map(_.name()).toList.sorted)(console.response(_))
    } yield RunningState.Continue

  override def showGroup(group: String): Task[RunningState] =
    for {
      admin       <- connection.admin
      description <- admin.describeGroup(group)
      members = description.members().asScala.toList
      _ <-
        console.response(
          s"Group: ${description.groupId()} "
            + s"- members:${members.size} (state:${description.state().name()})"
        )
      _ <-
        ZIO.foreach(members) { member =>
          val partitions = member.assignment().topicPartitions().asScala
          console.response(
            s"\tclientID:${member.clientId()} - consumerID:${member.consumerId()} "
              + s"- assigment(${partitions.size}): ${partitions.map(tp => s"${tp.topic()}#${tp.partition()}").mkString(",")}"
          )
        }
    } yield RunningState.Continue

  override def showGroups: Task[RunningState] =
    for {
      admin  <- connection.admin
      groups <- admin.listGroups
      _ <-
        ZIO.foreach {
          groups.toList
            .sortBy(_.groupId())
        } { group =>
          console.response(s"${group.groupId()} (state: ${group.state().map(_.name()).orElse("???")})")
        }
    } yield RunningState.Continue

  override def showCluster: Task[RunningState] =
    for {
      admin      <- connection.admin
      id         <- admin.describeClusterId
      controller <- admin.describeClusterController
      nodes      <- admin.describeClusterNodes
      _          <- console.response(s"cluster: $id - controller: ${controller.id()}")
      _ <-
        ZIO.foreach(nodes) { node =>
          console.response(s"\tNode #${node.id()} - ${node.host()}:${node.port()} - rack:${node.rack()}")
        }
    } yield RunningState.Continue

  override def createGroup(group: String, topic: String): Task[RunningState] =
    for {
      admin <- connection.admin
      _     <- admin.createGroup(group, topic)
    } yield RunningState.Continue

  override def createTopic(topic: String, partitionCount: Int, replicationCount: Option[Int]): Task[RunningState] =
    (
      for {
        admin    <- connection.admin
        replicas <- ZIO.fromOption(replicationCount).orElse(admin.describeClusterNodes.map(_.size))
        _        <- admin.createTopic(topic, partitionCount, replicas)
      } yield RunningState.Continue
    ).foldZIO(
      { case e: TopicExistsException =>
        console.error(s"topic already exists: $topic") *> RunningState.ContinueM
      },
      ZIO.succeed
    )

  override def deleteTopic(topic: String): Task[RunningState] =
    (
      for {
        admin <- connection.admin
        _     <- admin.deleteTopic(topic)
      } yield RunningState.Continue
    ).foldZIO(
      { case e: UnknownTopicOrPartitionException =>
        console.error(s"unknown topic: $topic") *> RunningState.ContinueM
      },
      ZIO.succeed
    )

  override def deleteGroup(group: String): Task[RunningState] =
    (
      for {
        admin <- connection.admin
        _     <- admin.deleteGroup(group)
      } yield RunningState.Continue
    ).foldZIO(
      { case e: GroupIdNotFoundException =>
        console.error(s"unknown group id: $group") *> RunningState.ContinueM
      },
      ZIO.succeed
    )

  override def select(fromTopic: String, last: Long): Task[RunningState] = {
    val records: Task[Iterable[ConsumerRecord[Array[Byte], Array[Byte]]]] =
      for {
        admin <- connection.admin
        partitions <-
          admin
            .describeTopic(fromTopic)
            .map(_.partitions().asScala.map(info => new TopicPartition(fromTopic, info.partition())).toList)
        consumer  <- connection.consumer
        _         <- consumer.assign(partitions)
        _         <- consumer.seekToEnd(partitions)
        positions <- consumer.position(partitions)
        newPositions = positions.map(p => Math.max(0L, p - last))
        _       <- consumer.seek(partitions, newPositions)
        records <- consumer.poll(Duration.fromMillis(1000))
        _       <- consumer.unsubscribe()
      } yield records.asScala

    records.flatMap { rs =>
      ZIO.foreach(rs) { record =>
        val headers = record.headers().asScala
        val headersSt =
          if (headers.isEmpty)
            "(empty)"
          else
            headers
              .map(h => s"${h.key()}: ${toStringEscape(h.value())}")
              .mkString("\n\t\t", "\n\t\t", "")
        val key   = toStringEscape(record.key())
        val value = toStringEscape(record.value())
        console.response(
          s"${record.topic()}@${record.partition()}#${record.offset()}-${record.timestamp()}\n"
            + s"\tHeaders:$headersSt\n"
            + s"\tKey: $key\n"
            + s"\tValue:"
        ) *> console.print(value)
      }
    } *> RunningState.ContinueM
  }

  override def insert(toTopic: String, key: String, value: String): Task[RunningState] =
    for {
      admin    <- connection.admin
      producer <- connection.producer
      topics   <- admin.listTopics
      _ <-
        if (topics.map(_.name()).toSet.contains(toTopic)) {
          producer
            .send(toTopic, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))
            .flatMap(metadata =>
              console.response(
                s"${metadata.topic()}@${metadata.partition()}#${metadata.offset()}-${metadata.timestamp()}"
              )
            )
        } else {
          console.error(s"sending to an unknown topic: $toTopic")
        }
    } yield RunningState.Continue

  private def toStringEscape(bytes: Array[Byte]): String =
    new String(bytes, StandardCharsets.UTF_8).map { c =>
      if (isPrintable(c)) c else '.'
    }

  private def isPrintable(c: Char): Boolean = {
    val block = Character.UnicodeBlock.of(c)

    (!Character.isISOControl(c))
    && (c != KeyEvent.CHAR_UNDEFINED)
    && (block != null)
    && (block != Character.UnicodeBlock.SPECIALS)
  }

}
