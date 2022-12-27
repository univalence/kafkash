package io.univalence.kafkash.command

import io.univalence.kafkash.command.Input.StringInput
import io.univalence.kafkash.command.Lexem.{
  IntValue,
  ParameterIdentifier,
  ParameterName,
  ParameterNameWithValue,
  StringValue
}
import io.univalence.kafkash.command.Parser.{int, StringParser}

import scala.collection.mutable.ArrayBuffer

object CommandParser {

  type LexemInput      = Input[Lexem]
  type LexemParser[+A] = Parser[Lexem, A]

  val Formats: Set[String] = Set("STRING", "HEX")

  def parse(input: String): Either[String, Command] = {
    val lexicalResult: ParseResult[Char, Either[String, Command]] =
      LexicalCommandParser
        .parse(input)
        .map { case (commandName, params) =>
          val syntaxicResult: ParseResult[Lexem, Command] = getParserOf(commandName).parse(Input(params)).commit

          syntaxicResult match {
            case ParseResult.Success(command, _) => Right(command)
            case ParseResult.Failure(message, i) => Left(s"Error: $message")
            case ParseResult.Error(message, i)   => Left(s"Error: $message")
          }
        }

    lexicalResult match {
      case ParseResult.Success(result, _)  => result
      case ParseResult.Failure(message, i) => Left(s"Error @ ${i.offset}: $message")
      case ParseResult.Error(message, i)   => Left(s"Error @ ${i.offset}: $message")
    }
  }

  def getParserOf(commandName: CommandName): LexemParser[Command] =
    commandName match {
      case CommandName("HELP")   => HelpCmd
      case CommandName("SELECT") => SelectCmd
      case CommandName("INSERT") => InsertCmd
      case CommandName("DELETE") => DeleteCmd
      case CommandName("CREATE") => CreateCmd
      case CommandName("SHOW")   => ShowCmd
      case CommandName("QUIT")   => Parser(Command.Quit)
      case _                     => Parser.fail(s"Unknown command: $commandName")
    }

  val string: LexemParser[String] = { (input: LexemInput) =>
    if (input.hasNext) {
      input.current.asStringValue
        .map(pname => ParseResult.Success(pname.value, input.next))
        .getOrElse(
          ParseResult.Failure(s"expected string - bad parameter: ${input.current}", input)
        )
    } else {
      ParseResult.Failure(s"expected string - end of input", input)
    }
  }

  val identifier: LexemParser[String] = { (input: LexemInput) =>
    if (input.hasNext) {
      input.current.asParameterIdentifier
        .map(pname => ParseResult.Success(pname.value, input.next))
        .getOrElse(
          ParseResult.Failure(s"expected identifier - bad parameter: ${input.current}", input)
        )
    } else {
      ParseResult.Failure(s"expected identifier - end of input", input)
    }
  }

  val HelpCmd: LexemParser[Command] = { (input: LexemInput) =>
    if (!input.hasNext) {
      ParseResult.Success(Command.Help(None), input)
    } else {
      var cmd = input.current.toString
      var i   = input.next
      while (i.hasNext) {
        cmd += " " + i.current.toString
        i = i.next
      }

      CommandType.values
        .map(ct => ct.commandName -> ct)
        .toMap
        .get(cmd.toUpperCase())
        .map(ct => ParseResult.Success(Command.Help(Some(ct)), i.next))
        .getOrElse(ParseResult.Failure(s"cannot find help for unknown command $cmd", i))
    }
  }

  val ShowCmd: LexemParser[Command] =
    oneWordOf("TOPICS")
      .map(_ => Command.ShowTopics)
      .or(word("GROUPS").map(_ => Command.ShowGroups))
      .or(word("CLUSTER").map(_ => Command.ShowCluster))
      .or(word("TOPIC") andThen identifier.commit map (topic => Command.ShowTopic(topic)))
      .or(word("GROUP").failMap { (_, i) =>
        if (i.hasNext)
          s"expected one of : CLUSTER, GROUP, GROUPS, TOPIC, TOPICS - bar parameter: ${i.current}"
        else
          s"expected one of : CLUSTER, GROUP, GROUPS, TOPIC, TOPICS - end of input"
      } andThen identifier.commit map (group => Command.ShowGroup(group)))

  val CreateCmd: LexemParser[Command] =
    (for {
      _          <- word("TOPIC")
      topic      <- identifier
      partitions <- namedValue("PARTITIONS")
      replicas   <- namedValue("REPLICAS").optional
    } yield Command.CreateTopic(topic, Some(partitions), replicas))
      .or(for {
        _          <- word("TOPIC")
        topic      <- identifier
        replicas   <- namedValue("REPLICAS").optional
        partitions <- namedValue("PARTITIONS").optional
      } yield Command.CreateTopic(topic, partitions, replicas))
      .or(for {
        _     <- word("GROUP")
        group <- identifier
        _     <- word("FOR")
        topic <- identifier
      } yield Command.CreateGroup(group, topic))

  val SelectCmd: LexemParser[Command] = {
    val firstPart: LexemParser[(String, String)] =
      for {
        format <-
          oneWordOf(Formats.toArray: _*)
            .map(_.toUpperCase)
            .failMap { (_, i) =>
              if (i.hasNext)
                s"expected format: ${Formats.mkString(", ")} - format unknown: ${i.current}"
              else
                s"expected format: ${Formats.mkString(", ")} - end of input"
            }
        _     <- word("FROM")
        topic <- identifier
      } yield (format, topic)

    (
      for {
        part <- firstPart
        format = part._1
        topic  = part._2
        _ <- word("FOLLOWS")
      } yield Command.SelectFollow(topic, format)
    ).or(
      for {
        part <- firstPart
        format = part._1
        topic  = part._2
        count <-
          namedValue("LAST")
            .failMap((_, _) => "expected FOLLOWS or LAST: n at the end of SELECT command")
      } yield Command.Select(topic, format, count)
    )
  }

  val InsertCmd: LexemParser[Command] =
    for {
      _     <- oneWordOf("INTO")
      topic <- identifier
      _     <- oneWordOf("KEY")
      key   <- string
      _     <- oneWordOf("VALUE")
      value <- string
    } yield Command.Insert(topic, key, value)

  val DeleteCmd: LexemParser[Command] =
    (word("TOPIC") andThen identifier map (topic => Command.DeleteTopic(topic)))
      .or(word("GROUP") andThen identifier map (group => Command.DeleteGroup(group)))

  def word(name: String): LexemParser[String] = { (input: LexemInput) =>
    if (input.hasNext) {
      input.current.asParameterIdentifier
        .filter(pname => name == pname.value.toUpperCase())
        .map(pname => ParseResult.Success(pname.value, input.next))
        .getOrElse(
          ParseResult.Failure(s"expected $name - bad parameter: ${input.current}", input)
        )
    } else {
      ParseResult.Failure(s"expected one of: $name - end of input", input)
    }
  }

  def oneWordOf(names: String*): LexemParser[String] = { (input: LexemInput) =>
    val inNames = Set.from(names)

    if (input.hasNext) {
      input.current.asParameterIdentifier
        .filter(pname => inNames.contains(pname.value.toUpperCase()))
        .map(pname => ParseResult.Success(pname.value, input.next))
        .getOrElse(
          ParseResult.Failure(s"expected one of: ${inNames.mkString(", ")} - bad parameter: ${input.current}", input)
        )
    } else {
      ParseResult.Failure(s"expected one of: ${inNames.mkString(", ")} - end of input", input)
    }
  }

  def namedValue(name: String): LexemParser[Int] = { (input: LexemInput) =>
    if (input.hasNext) {
      input.current.asParameterNameWithValue
        .filter(prm => prm.name.toUpperCase() == name)
        .map(prm => ParseResult.Success(prm.value, input.next))
        .getOrElse(
          ParseResult.Failure(s"parameter $name with value expected - bad parameter: ${input.current}", input)
        )
    } else {
      ParseResult.Failure(s"parameter $name with value expected - end of input", input)
    }
  }

}

object LexicalCommandParser {
  val StringDelimiter = '\''

  def parse(line: String): ParseResult[Char, (CommandName, List[Lexem])] = commandLineParser.commit.parse(Input(line))

  val word: StringParser[String] = { (input: StringInput) =>
    var i = input
    var w = ""
    while (i.hasNext && i.current.isLetter) {
      w += i.current
      i = i.next
    }

    if (w.nonEmpty)
      ParseResult.Success(w, i)
    else
      ParseResult.Failure("Word expected", input)
  }

  val identifier: StringParser[String] = { (input: StringInput) =>
    val specials = Set('-', '_')

    var i          = input
    var identifier = ""
    while (i.hasNext && (i.current.isLetter || specials.contains(i.current))) {
      identifier += i.current
      i = i.next
    }

    if (identifier.nonEmpty)
      ParseResult.Success(identifier, i)
    else
      ParseResult.Failure("Identifier expected", input)
  }

  def commandName: StringParser[CommandName] = word.map(word => CommandName(word.toUpperCase))

  def parameterWithValue: StringParser[Lexem.ParameterNameWithValue] =
    for {
      name  <- word
      _     <- Parser.char(':')
      _     <- Parser.skipSpaces
      value <- Parser.int
    } yield Lexem.ParameterNameWithValue(name, value)

  val string: StringParser[StringValue] = Parser.string(StringDelimiter).map(s => Lexem.StringValue(s))

  val int: StringParser[IntValue] = Parser.int.map(i => IntValue(i))

  val parameterIdentifier: StringParser[ParameterIdentifier] = identifier.map(i => Lexem.ParameterIdentifier(i))

  val parameter: StringParser[Lexem] = string or int or parameterWithValue or parameterIdentifier

  val commandLineParser: StringParser[(CommandName, List[Lexem])] =
    for {
      _       <- Parser.skipSpaces
      command <- commandName
      params  <- (Parser.skipSpaces.andThen(parameter)).repeat
      _       <- Parser.skipSpaces
    } yield (command, params)

}

case class CommandName(name: String)

enum Lexem {
  case ParameterName(name: String)
  case ParameterIdentifier(value: String)
  case ParameterNameWithValue(name: String, value: Int)
  case StringValue(value: String)
  case IntValue(value: Int)

  override def toString: String =
    this match {
      case ParameterName(name)                 => name
      case ParameterIdentifier(value)          => value
      case ParameterNameWithValue(name, value) => s"$name: $value"
      case StringValue(value)                  => s"'$value'"
      case IntValue(value)                     => value.toString
    }

  def asParameterIdentifier: Option[ParameterIdentifier] =
    this match {
      case l @ ParameterIdentifier(_) => Some(l)
      case _                          => None
    }

  def asParameterNameWithValue: Option[ParameterNameWithValue] =
    this match {
      case l @ ParameterNameWithValue(_, _) => Some(l)
      case _                                => None
    }

  def asStringValue: Option[StringValue] =
    this match {
      case l @ StringValue(_) => Some(l)
      case _                  => None
    }

  def asIntValue: Option[IntValue] =
    this match {
      case l @ IntValue(_) => Some(l)
      case _               => None
    }

}
