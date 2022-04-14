package io.univalence.kafkash.command

import scala.util.Try
import scala.util.matching.Regex
import scala.util.parsing.combinator.*

object CommandParser extends RegexParsers {
  override def skipWhitespace = false

  def s: Parser[Unit] = """[ \t]+""".r ^^^ ()

  def keyword(k: String): Parser[String] =
    ("""[a-zA-Z:\-_]+""".r ^^ { _.toUpperCase })
      .flatMap { s =>
        if (s == k.toUpperCase)
          OnceParser(i => Success(k, i))
        else
          OnceParser(i => Failure(s"expected $k, got: $s", i))
      } | failure(s"$k expected")

  def parseString: Parser[String] =
    ("\'" + """([^'\x00-\x1F\x7F\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\'").r ^^ { s =>
      s.substring(1, s.length - 1)
        .replace("\\'", "'")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\b", "\b")
        .replace("\\f", "\f")
        .replace("\\\\", "\\")
        .replace("\\", "\"")
    }

  def parseInt: Parser[Int]     = """[1-9][0-9]*""".r ^^ { _.toInt }
  def parseName: Parser[String] = """[a-zA-Z0-9\-_]+""".r
  def parseAny: Parser[String]  = guard(""".+""".r)

  def quit: Parser[Command] = keyword("QUIT") ^^^ Command.Quit
  def help: Parser[Command] =
    keyword("HELP") ~> commit(
      (s ~>
        (
          (keyword("QUIT") ^^^ CommandType.Quit)
            | (keyword("HELP") ^^^ CommandType.Help)
            | (keyword("SHOW") ~ s ~ keyword("TOPICS") ^^^ CommandType.ShowTopics)
            | (keyword("SHOW") ~ s ~ keyword("GROUPS") ^^^ CommandType.ShowGroups)
            | (keyword("SHOW") ~ s ~ keyword("CLUSTER") ^^^ CommandType.ShowCluster)
            | (keyword("SHOW") ~ s ~ keyword("TOPIC") ^^^ CommandType.ShowTopic)
            | (keyword("SHOW") ~ s ~ keyword("GROUP") ^^^ CommandType.ShowGroup)
            | (keyword("CREATE") ~ s ~ keyword("TOPIC") ^^^ CommandType.CreateTopic)
            | (keyword("CREATE") ~ s ~ keyword("GROUP") ^^^ CommandType.CreateGroup)
            | (keyword("DELETE") ~ s ~ keyword("TOPIC") ^^^ CommandType.DeleteTopic)
            | (keyword("DELETE") ~ s ~ keyword("GROUP") ^^^ CommandType.DeleteGroup)
            | (keyword("SELECT") ^^^ CommandType.Select)
            | (keyword("INSERT") ^^^ CommandType.Insert)
            | parseAny ~> fail("unknown help topic")
        )).? ^^ { command => Command.Help(command) }
    )

  def show: Parser[Command] =
    keyword("SHOW") ~> s ~> commit(
      (keyword("TOPICS") ^^^ Command.ShowTopics)
        | (keyword("GROUPS") ^^^ Command.ShowGroups)
        | (keyword("CLUSTER") ^^^ Command.ShowCluster)
        | (keyword("TOPIC") ~> s ~> (parseName ^^ { name => Command.ShowTopic(name) }))
        | (keyword("GROUP") ~> s ~> (parseName ^^ { name => Command.ShowGroup(name) }))
        | parseAny ~> fail("unknown item to show")
    )

  def create: Parser[Command] =
    keyword("CREATE") ~> s ~> commit(
      ((
        keyword("TOPIC") ~> s ~> parseName ~ (
          (((s ~> keyword("REPLICAS:") ~> s.? ~> parseInt)
            ~ (s ~> keyword("PARTITIONS:") ~> s.? ~> parseInt).?) ^^ { case replicas ~ partitions =>
            (partitions, Some(replicas))
          })
            | (((s ~> keyword("PARTITIONS:") ~> s.? ~> parseInt)
              ~ (s ~> keyword("REPLICAS:") ~> s.? ~> parseInt).?) ^^ { case partitions ~ replicas =>
              (Some(partitions), replicas)
            })
            | (parseAny ~> fail("unknown topic creation option"))
            | success((Option.empty[Int], Option.empty[Int]))
        )
      ) ^^ { case name ~ (partitions, replicas) =>
        Command.CreateTopic(name = name, partitions = partitions, replicas = replicas)
      })
        | ((keyword("GROUP") ~> s ~> parseName ~ (s ~> keyword("FOR") ~> s ~> parseName)) ^^ { case name ~ topic =>
          Command.CreateGroup(name, topic)
        })
        | parseAny ~> fail("unknown item to create")
    )

  def delete: Parser[Command] =
    keyword("DELETE") ~> s ~> commit(
      ((keyword("TOPIC") ~> s ~> parseName) ^^ { topic => Command.DeleteTopic(topic) })
        | ((keyword("GROUP") ~> s ~> parseName) ^^ { group => Command.DeleteGroup(group) })
        | parseAny ~> fail("unknown item to delete")
    )

  def select: Parser[Command] =
    keyword("SELECT") ~> s ~> keyword("FROM") ~> s ~> commit(
      (parseName ~ (s ~> keyword("LAST") ~> (s ~> parseInt).?)) ^^ { case topic ~ delta =>
        Command.Select(topic, delta.getOrElse(1).toLong)
      }
        | (parseName ~ (s ~> keyword("FOLLOW"))) ^^ { case topic ~ _ =>
          Command.SelectFollow(topic)
        }
    )

  def insert: Parser[Command] =
    keyword("INSERT") ~> s ~> keyword("INTO") ~> s ~> commit(
      parseName
        ~ (s ~> keyword("KEY") ~> s ~> parseString)
        ~ (s ~> keyword("VALUE") ~> s ~> parseString) ^^ { case topic ~ key ~ value =>
          Command.Insert(toTopic = topic, key = key, value = value)
        }
    )

  def command: Parser[Command] = commit(phrase(quit | help | show | create | delete | select | insert))

  def parseCommand(rawCommand: String): Try[Command] =
    parse(command, rawCommand) match {
      case Success(result, _) => scala.util.Success(result)
      case Failure(reason, _) => scala.util.Failure(new NoSuchElementException(reason))
      case Error(reason, _)   => scala.util.Failure(new NoSuchElementException(reason))
    }

  def fail[A](msg: String): Parser[A] =
    Parser { in =>
      val rest = in.source.toString.split("\\s+", 2)(1)
      Failure(s"$msg: $rest", in)
    }

  def log_[T](p: => Parser[T])(name: String): Parser[T] =
    Parser { in =>
      println(s"trying $name at ${in.source.toString.substring(in.offset)}")
      val r = p(in)
      println(name + " --> " + r)
      r
    }

}
