package io.univalence.kafkash

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import io.univalence.kafkash.wrapper.{ZAdminClient, ZConsumer, ZProducer}

import zio.*

object KafkaConnection {

  trait Connection {
    val admin: Task[ZAdminClient]

    val consumer: Task[ZConsumer[Array[Byte], Array[Byte]]]

    val producer: Task[ZProducer[Array[Byte], Array[Byte]]]

    def close: Task[Unit] = admin.map(_.close)
  }

  case class ConnectionLive(
      zadminClient: ZAdminClient,
      zconsumer:    ZConsumer[Array[Byte], Array[Byte]],
      zproducer:    ZProducer[Array[Byte], Array[Byte]]
  ) extends Connection {
    override val admin: UIO[ZAdminClient] = UIO.succeed(zadminClient)

    override val consumer: Task[ZConsumer[Array[Byte], Array[Byte]]] = UIO.succeed(zconsumer)

    override val producer: Task[ZProducer[Array[Byte], Array[Byte]]] = UIO.succeed(zproducer)
  }
  object ConnectionLive {
    import scala.jdk.CollectionConverters.*

    def make(
        adminProperties: Map[String, AnyRef],
        consumerProperties: Map[String, AnyRef],
        producerProperties: Map[String, AnyRef]
    ): Task[ConnectionLive] =
      Task.attempt {
        val adminClient = AdminClient.create(adminProperties.asJava)
        val consumer    = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties.asJava)
        val producer    = new KafkaProducer[Array[Byte], Array[Byte]](producerProperties.asJava)

        ConnectionLive(new ZAdminClient(adminClient), new ZConsumer(consumer), new ZProducer(producer))
      }
  }

  def layer(
      adminProperties: Map[String, AnyRef],
      consumerProperties: Map[String, AnyRef],
      producerProperties: Map[String, AnyRef]
  ): ZLayer[Any, Throwable, ConnectionLive] =
    ZLayer.scoped {
      ZIO.acquireRelease(ConnectionLive.make(adminProperties, consumerProperties, producerProperties))(
        _.close.foldZIO(
          e => ZIO.succeed(e.printStackTrace()),
          ZIO.succeed
        )
      )
    }

  object Connection {

    val admin: RIO[Connection, ZAdminClient] = ZIO.serviceWithZIO[Connection](_.admin)

    val consumer: RIO[Connection, ZConsumer[Array[Byte], Array[Byte]]] = ZIO.serviceWithZIO[Connection](_.consumer)

    val producer: RIO[Connection, ZProducer[Array[Byte], Array[Byte]]] = ZIO.serviceWithZIO[Connection](_.producer)

    def close: RIO[Connection, Unit] = ZIO.serviceWithZIO[Connection](_.close)

  }

}
