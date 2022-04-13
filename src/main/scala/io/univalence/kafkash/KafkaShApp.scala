package io.univalence.kafkash

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, KafkaAdminClient}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.jline.reader.{EndOfFileException, UserInterruptException}
import org.jline.terminal.{Terminal, TerminalBuilder}

import zio.*

import scala.util.{Failure, Success}

import java.io.IOException
import java.time.Instant

object KafkaShApp extends ZIOAppDefault {
  import KafkaConnection.Connection
  import KafkaInterpreter.Interpreter
  import KafkaShConsole.Console

  import io.univalence.kafkash.command.*

  val defaultBootstrapServers = "localhost:9092"
  val defaultPartitionCount   = 4

  override def run = {
    val applicationName = s"${io.univalence.kafkash.BuildInfo.name}-v${io.univalence.kafkash.BuildInfo.version}"

    val ConnectionLayer: TaskLayer[Connection] =
      KafkaConnection.layer(
        Map(
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> defaultBootstrapServers,
          AdminClientConfig.CLIENT_ID_CONFIG         -> s"$applicationName-adminclient-${Instant.now().toEpochMilli}"
        ),
        Map(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> defaultBootstrapServers,
          ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG -> "false",
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[ByteArrayDeserializer].getCanonicalName,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getCanonicalName
        ),
        Map(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG      -> defaultBootstrapServers,
          ProducerConfig.ACKS_CONFIG                   -> "all",
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG   -> classOf[ByteArraySerializer].getCanonicalName,
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getCanonicalName
        )
      )

    (
      for {
        _ <- Console(_.print(applicationName))
        _ <- Console(_.response("type HELP for available commands"))
        _ <- repLoop
      } yield ()
    ).provide(
      zio.Console.live,
      terminalLayer(applicationName),
      ConnectionLayer,
      KafkaShConsole.layer,
      KafkaInterpreter.layer
    )
  }

  def repLoop: ZIO[Connection with Console with Interpreter, Throwable, Unit] =
    (
      for {
        command        <- readCommand(">")
        shouldContinue <- execute(command)
      } yield shouldContinue
    ).foldZIO(
      {
        case CommandIssue.Empty =>
          RunningState.ContinueM
        case CommandIssue.SyntaxError(command) =>
          Console(_.error(s"Syntax error in command: $command")) *> RunningState.ContinueM
        case CommandIssue.GenericError(e) =>
          e.printStackTrace()
          RunningState.StopM
      },
      UIO.succeed
    ).repeatUntil(_ == RunningState.Stop)
      .unit

  def readCommand(prompt: String): ZIO[Console, CommandIssue, Command] =
    Console(_.read(">"))
      .foldZIO(
        {
          case e: EndOfFileException =>
            ZIO.succeed(Command.Quit)
          case e: UserInterruptException =>
            ZIO.fail(CommandIssue.Empty)
          case e: Throwable =>
            e.printStackTrace()
            ZIO.fail(CommandIssue.GenericError(e))
        },
        line =>
          if (line.isEmpty)
            ZIO.fail(CommandIssue.Empty)
          else
            CommandParser.parseCommand(line) match {
              case Success(cmd)                       => ZIO.succeed(cmd)
              case Failure(e: NoSuchElementException) => ZIO.fail(CommandIssue.SyntaxError(e.getMessage))
              case Failure(e) =>
                e.printStackTrace()
                ZIO.fail(CommandIssue.GenericError(e))
            }
      )

  /**
   * Execute a command.
   *
   * @return
   *   indicate if the application should continue or stop.
   */
  def execute(command: Command): RIO[Connection with Console with Interpreter, RunningState] =
    command match {
      case Command.Quit                      => RunningState.StopM
      case Command.Help(None)                => displayHelp *> RunningState.ContinueM
      case Command.Help(Some(commandType))   => displayCommandHelp(commandType) *> RunningState.ContinueM
      case Command.ShowTopic(topic)          => Interpreter(_.showTopic(topic))
      case Command.ShowTopics                => Interpreter(_.showTopics)
      case Command.ShowGroup(group)          => Interpreter(_.showGroup(group))
      case Command.ShowGroups                => Interpreter(_.showGroups)
      case Command.ShowCluster               => Interpreter(_.showCluster)
      case Command.DeleteTopic(topic)        => Interpreter(_.deleteTopic(topic))
      case Command.DeleteGroup(group)        => Interpreter(_.deleteGroup(group))
      case Command.CreateGroup(group, topic) => Interpreter(_.createGroup(group, topic))
      case Command.CreateTopic(topic, partitions, replicats) =>
        Interpreter(_.createTopic(topic, partitions.getOrElse(defaultPartitionCount), None))
      case Command.Select(fromTopic, last)     => Interpreter(_.select(fromTopic, last))
      case Command.Insert(toTopic, key, value) => Interpreter(_.insert(toTopic, key, value))
    }

  lazy val displayHelp: RIO[Console, Unit] = {
    val columns = 3

    val commands: List[String] =
      CommandType.values.toList
        .map(_.commandName)
        .sorted

    val groupSize = (commands.size / columns.toDouble).ceil.toInt

    val cmdGroups: List[List[String]] =
      commands
        .grouped(groupSize)
        .toList

    val sizeMax           = cmdGroups.map(_.size).max
    val cmdBalancedGroups = cmdGroups.map(_.padTo(sizeMax, "")).transpose

    val commandsStr =
      cmdBalancedGroups
        .map(_.map(_.padTo(20, ' ')).mkString(" "))
        .mkString("\t", "\n\t", "")

    Console(_.response(s"Available commands:\n$commandsStr"))
  }

  def displayCommandHelp(commandType: CommandType): ZIO[Console, Throwable, Unit] =
    Console(_.response(s"\t${commandType.usage}\t${commandType.description}"))

  def terminalLayer(terminalName: String): TaskLayer[Terminal] = {
    val terminal: Terminal =
      TerminalBuilder
        .builder()
        .name(terminalName)
        .system(true)
        .build()

    ZLayer.scoped {
      ZIO.fromAutoCloseable(Task.succeed(terminal))
    }
  }

  enum RunningState {
    case Continue
    case Stop
  }
  object RunningState {
    val ContinueM: UIO[RunningState] = UIO.succeed(RunningState.Continue)
    val StopM: UIO[RunningState]     = UIO.succeed(RunningState.Stop)
  }

}
