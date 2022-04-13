package io.univalence.kafkash.command

import org.scalatest.funsuite.AnyFunSuiteLike

import scala.util.Try

class CommandParserTest extends AnyFunSuiteLike {

  val parser: CommandParser.type = CommandParser
  import parser.{Error, Failure, Success}

  test("should parse quit") {
    val result: Try[Command] = parser.parseCommand("quit")

    assert(result === scala.util.Success(Command.Quit))
  }

  test("should parse show topics") {
    val result = parser.parseCommand("show  topics")

    assert(result == scala.util.Success(Command.ShowTopics))
  }

  test("should parse show topic with name") {
    val result = parser.parseCommand("show  topic topic-name")

    assert(result == scala.util.Success(Command.ShowTopic("topic-name")))
  }

  test("should parse command help") {
    val result = parser.parseCommand("help")

    assert(result == scala.util.Success(Command.Help(None)))
  }

  test("should parse command help help") {
    val result = parser.parseCommand("help help")

    assert(result == scala.util.Success(Command.Help(Some(CommandType.Help))))
  }

  test("should parse command insert") {
    val result = parser.parseCommand("""INSERT INTO topic KEY 'abc\'\'31' VALUE '{"msg": "hello"}'""")

    assert(result == scala.util.Success(Command.Insert("topic", "abc''31", """{"msg": "hello"}""")))
  }

}
