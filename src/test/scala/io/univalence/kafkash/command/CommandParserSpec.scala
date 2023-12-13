package io.univalence.kafkash.command

import zio.test._

object CommandParserSpec extends ZIOSpecDefault {

  override def spec =
    suite("Command parser")(
      suite("Help")(
        test("should parse simple help") {
          val command = "help"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.Help(None)))
        },
        test("should parse help of command") {
          val command = "help select"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.Help(Some(CommandType.Select))))
        },
        test("should not parse help on unknown command") {
          val command = "help whatever"
          val result  = CommandParser.parse(command)

          assertTrue(result == Left(s"Error: cannot find help for unknown command whatever"))
        }
      ),
      suite("Select")(
        test("should parse select follows command") {
          val command = "select string from topic follows"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.SelectFollow("topic", "STRING")))
        },
        test("should parse select hex follows command") {
          val command = "select hex from topic follows"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.SelectFollow("topic", "HEX")))
        },
        test("should parse select last command") {
          val command = "select string from topic last 5"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.Select("topic", "STRING", 5)))
        },
        test("should not parse select command with unknown format") {
          val command = "select whatever from topic last 5"
          val result  = CommandParser.parse(command)

          assertTrue(result == Left("Error: expected format: STRING, HEX - format unknown: whatever"))
        },
        test("should not parse select command without end") {
          val command = "select string from topic"
          val result  = CommandParser.parse(command)

          assertTrue(result == Left("Error: expected FOLLOWS or LAST n at the end of SELECT command"))
        }
      ),
      suite("Insert")(
        test("should parse insert command") {
          val command = "insert into topic key 'abc' value 'def'"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.Insert("topic", "abc", "def")))
        }
      ),
      suite("Create")(
        test("should parse simple create topic command") {
          val command = "create topic topic"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.CreateTopic("topic", None, None)))
        },
        test("should parse create topic with partitions command") {
          val command = "create topic topic partitions: 4"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.CreateTopic("topic", Some(4), None)))
        },
        test("should parse create topic with replicas command") {
          val command = "create topic topic replicas: 3"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.CreateTopic("topic", None, Some(3))))
        },
        test("should parse create topic with partitions and replicas command") {
          val command = "create topic topic partitions: 4 replicas: 3"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.CreateTopic("topic", Some(4), Some(3))))
        },
        test("should parse create topic with replicas and partitions command") {
          val command = "create topic topic replicas: 3 partitions: 4"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.CreateTopic("topic", Some(4), Some(3))))
        },
        test("should parse create group command") {
          val command = "create group group for topic"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.CreateGroup("group", "topic")))
        }
      ),
      suite("Show")(
        test("should parse show cluster") {
          val command = "show cluster"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.ShowCluster))
        },
        test("should parse show topics") {
          val command = "show topics"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.ShowTopics))
        },
        test("should parse show groups") {
          val command = "show groups"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.ShowGroups))
        },
        test("should parse show topic") {
          val command = "show topic topic"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.ShowTopic("topic")))
        },
        test("should parse show group") {
          val command = "show group group"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.ShowGroup("group")))
        },
        test("should not parse show topic with no topic name") {
          val command = "show topic"
          val result  = CommandParser.parse(command)

          assertTrue(result == Left("Error: expected identifier - end of input"))
        }
      ),
      suite("Delete")(
        test("should parse delete topic command") {
          val command = "delete topic topic"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.DeleteTopic("topic")))
        },
        test("should parse delete group command") {
          val command = "delete group group"
          val result  = CommandParser.parse(command)

          assertTrue(result == Right(Command.DeleteGroup("group")))
        }
      )
    )

}
