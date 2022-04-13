package io.univalence.kafkash

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import zio.*

class ZProducer[K, V](producer: KafkaProducer[K, V]) extends ZWrapped[KafkaProducer[K, V]](producer) {

  import scala.jdk.CollectionConverters.*

  def send(topic: String, key: K, value: V): Task[RecordMetadata] = {
    val record = new ProducerRecord[K, V](topic, key, value)

    for {
      metadata <- ZIO.fromFutureJava(producer.send(record))
    } yield metadata
  }

}
