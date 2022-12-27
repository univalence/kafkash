package io.univalence.kafkash.command

enum ParseResult[I, +A](val input: Input[I]) {
  case Success(value: A, override val input: Input[I]) extends ParseResult[I, A](input)
  case Failure(reason: String, override val input: Input[I]) extends ParseResult[I, Nothing](input)
  case Error(reason: String, override val input: Input[I]) extends ParseResult[I, Nothing](input)

  def getOrElse[A1 >: A](default: A1): A1 =
    this match {
      case Success(value, _) => value
      case _                 => default
    }

  def isSuccess: Boolean =
    this match {
      case Success(_, _) => true
      case _             => false
    }

  def isFailure: Boolean =
    this match {
      case Failure(_, _) => true
      case _             => false
    }

  def isError: Boolean =
    this match {
      case Error(_, _) => true
      case _           => false
    }

  def flatMap[B](f: A => ParseResult[I, B]): ParseResult[I, B] =
    this match {
      case Success(value, _)      => f(value)
      case Failure(reason, input) => Failure(reason, input)
      case Error(reason, input)   => Error(reason, input)
    }

  def flatMapS[B](f: (A, Input[I]) => ParseResult[I, B]): ParseResult[I, B] =
    this match {
      case Success(value, input)  => f(value, input)
      case Failure(reason, input) => Failure(reason, input)
      case Error(reason, input)   => Error(reason, input)
    }

  def map[B](f: A => B): ParseResult[I, B] =
    this match {
      case Success(value, input)  => Success(f(value), input)
      case Failure(reason, input) => Failure(reason, input)
      case Error(reason, input)   => Error(reason, input)
    }

  def failMap(f: (String, Input[I]) => String): ParseResult[I, A] =
    this match {
      case Success(_, _)          => this
      case Failure(reason, input) => Failure(f(reason, input), input)
      case Error(_, _)            => this
    }

  def commit: ParseResult[I, A] =
    this match {
      case Success(_, input) =>
        if (input.hasNext)
          Error("failure in parsing the whole input", input)
        else
          this
      case Failure(reason, input) => Error(reason, input)
      case Error(_, _)            => this
    }
}
