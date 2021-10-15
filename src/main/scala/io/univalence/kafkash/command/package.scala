package io.univalence.kafkash

import java.time._

package object command {

  val defaultTimeout: Duration = Duration.ofSeconds(5)

  def toLocalDateTime(timestamp: Long): LocalDateTime =
    Instant
      .ofEpochMilli(timestamp)
      .atZone(ZoneId.systemDefault())
      .toLocalDateTime

}
