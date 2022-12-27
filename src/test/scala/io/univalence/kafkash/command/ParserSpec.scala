package io.univalence.kafkash.command

import io.univalence.kafkash.command.Input.StringInput
import io.univalence.kafkash.command.Parser.StringParser

import zio.test.*

object ParserSpec extends ZIOSpecDefault {

  override def spec =
    suite("Parser spec")(
      test("should parse a word") {
        val result = readWord.parse(Input("hello"))
        assertTrue(result == ParseResult.Success("hello", Input("hello", 5)))
      },
      test("should not parse an empty word") {
        val result = readWord.parse(Input(""))
        assertTrue(result == ParseResult.Failure("Word expected", Input("", 0)))
      },
      test("should compose two parsers and skip the first one") {
        val result = (Parser.skipSpaces.andThen(readWord)).parse(Input("  hello"))
        assertTrue(result == ParseResult.Success("hello", Input("  hello", 7)))
      },
      test("should compose three parsers") {
        val result =
          (for {
            a <- readWord
            _ <- Parser.skipSpaces
            b <- readWord
          } yield (a, b))
            .parse(Input("hello jon"))
        assertTrue(result == ParseResult.Success(("hello", "jon"), Input("hello jon", 9)))
      },
      test("should have optional parser") {
        val result = (Parser.skipSpaces.andThen(readWord)).repeat.parse(Input("Hello Jon Barry"))
        assertTrue(result == ParseResult.Success(List("Hello", "Jon", "Barry"), Input("Hello Jon Barry", 15)))
      },
      suite("String parser")(
        test("continue") {
          (assertTrue(Parser.shouldContinue(Input("h"), '\''))
            && assertTrue(Parser.shouldContinue(Input("''"), '\''))
            && assertTrue(!Parser.shouldContinue(Input("'"), '\''))
            && assertTrue(!Parser.shouldContinue(Input("' "), '\''))
            )
        },
        test("should parse string with delimiters") {
          val result = (Parser.string('\'').parse(Input("'hello'")))
          assertTrue(result == ParseResult.Success("hello", Input("'hello'", 7)))
        },
        test("should parse string with delimiters and escape delimiter") {
          val result = (Parser.string('\'').parse(Input("'hello''s'")))
          assertTrue(result == ParseResult.Success("hello's", Input("'hello''s'", 10)))
        },

      )
    )

  def readWord: StringParser[String] = { (input: StringInput) =>
    var i = input
    var cmd = ""
    while (i.hasNext && i.current.isLetter) {
      cmd += i.current
      i = i.next
    }

    if (cmd.nonEmpty)
      ParseResult.Success(cmd, i)
    else
      ParseResult.Failure("Word expected", input)
  }

}
