package io.univalence.kafkash

import org.jline.terminal.Terminal

import zio.*

object KafkaInterpreter {
  import KafkaConnection.Connection
  import KafkaShApp.RunningState
  import KafkaShConsole.Console

  trait Interpreter {
    def showTopic(topic: String): Task[RunningState]

    def showTopics: Task[RunningState]

    def showGroup(group: String): Task[RunningState]

    def showGroups: Task[RunningState]

    def showCluster: Task[RunningState]

    def createTopic(topic: String, partitionCount: Int, replicationCount: Option[Int]): Task[RunningState]

    def createGroup(goup: String, topic: String): Task[RunningState]

    def deleteTopic(topic: String): Task[RunningState]

    def deleteGroup(group: String): Task[RunningState]

    def selectFollow(fromTopic: String): Task[RunningState]

    def select(fromTopic: String, last: Long): Task[RunningState]

    def insert(toTopic: String, key: String, value: String): Task[RunningState]
  }

  object Interpreter extends Accessible[Interpreter]

  val layer: URLayer[Connection with Console, Interpreter] =
    ZLayer.fromFunction[Connection, Console, Interpreter](InterpreterLive.apply)

}
