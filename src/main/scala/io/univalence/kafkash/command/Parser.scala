package io.univalence.kafkash.command

import io.univalence.kafkash.command.Input.StringInput

import scala.collection.mutable.ArrayBuffer

trait Parser[I, +A] {
  def parse(input: Input[I]): ParseResult[I, A]

  def flatMap[B](f: A => Parser[I, B]): Parser[I, B] =
    (input: Input[I]) => parse(input).flatMapS((a, input1) => f(a).parse(input1))

  def map[B](f: A => B): Parser[I, B] = (input: Input[I]) => parse(input).map(f)

  def failMap(f: (String, Input[I]) => String): Parser[I, A] = (input: Input[I]) => parse(input).failMap(f)

  def andThen[B](p: Parser[I, B]): Parser[I, B] = flatMap(_ => p)

  def commit: Parser[I, A] = (input: Input[I]) => parse(input).commit

  def optional: Parser[I, Option[A]] = { (input: Input[I]) =>
    parse(input) match {
      case ParseResult.Success(value, input1) => ParseResult.Success(Some(value), input1)
      case ParseResult.Failure(_, _)          => ParseResult.Success(None, input)
      case ParseResult.Error(reason, input1)  => ParseResult.Error(reason, input1)
    }
  }

  def repeat: Parser[I, List[A]] = { (input: Input[I]) =>
    val values = ArrayBuffer.empty[A]
    var result = parse(input)

    while (result.isSuccess) {
      result match {
        case ParseResult.Success(value, _) => values.append(value)
        case _                             => ()
      }

      result = parse(result.input)
    }

    ParseResult.Success(values.toList, result.input)
  }

  def or[A1 >: A](p: Parser[I, A1]): Parser[I, A1] = { (input: Input[I]) =>
    parse(input) match {
      case ParseResult.Failure(_, _)          => p.parse(input)
      case ParseResult.Success(value, input1) => ParseResult.Success(value, input1)
      case ParseResult.Error(reason, input1)  => ParseResult.Error(reason, input1)
    }
  }
}
object Parser {
  type StringParser[+A] = Parser[Char, A]

  def apply[I, A](a: => A): Parser[I, A] = (input: Input[I]) => ParseResult.Success(a, input)

  def char(c: Char): StringParser[Char] = { (input: StringInput) =>
    if (input.hasNext && input.current == c)
      ParseResult.Success(c, input.next)
    else if (input.hasNext)
      ParseResult.Failure(s"Found: ${input.current} - Expected: $c", input)
    else
      ParseResult.Failure(s"Found: (end of input) - Expected: $c", input)
  }

  def int: StringParser[Int] = { (input: StringInput) =>
    var i     = input
    var value = ""
    while (i.hasNext && i.current.isDigit) {
      value += i.current
      i = i.next
    }

    if (value.nonEmpty)
      ParseResult.Success(value.toInt, i)
    else
      ParseResult.Failure("Integer expected", input)
  }

  def shouldContinue(input: StringInput, delimiter: Char): Boolean =
    (
      (input.hasNext && input.current != delimiter)
        || (input.hasNext && input.current == delimiter && input.next.hasNext && input.next.current == delimiter)
    )

  def string(delimiter: Char): StringParser[String] = { (input: StringInput) =>
    if (!input.hasNext) ParseResult.Failure("String expected", input)
    else if (input.current != delimiter) ParseResult.Failure(s"String should starts with \"$delimiter\"", input)
    else {
      var i = input.next
      var s = ""
      while (shouldContinue(i, delimiter)) {
        s += i.current
        if (i.current == delimiter && i.next.hasNext && i.next.current == delimiter)
          i = i.next.next
        else
          i = i.next
      }

      if (!i.hasNext || i.current != delimiter) {
        ParseResult.Failure(s"String should ends with \"$delimiter\"", input)
      } else
        ParseResult.Success(s, i.next)
    }
  }

  def skipSpaces: StringParser[Unit] = { (input: StringInput) =>
    var i = input

    while (i.hasNext && i.current.isSpaceChar)
      i = i.next

    ParseResult.Success((), i)
  }

  def fail[I, A](message: String): Parser[I, A] = (input: Input[I]) => ParseResult.Failure(message, input)

}
