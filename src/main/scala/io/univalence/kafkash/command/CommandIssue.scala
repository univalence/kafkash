package io.univalence.kafkash.command

enum CommandIssue {
  case SyntaxError(line: String)
  case GenericError(throwable: Throwable)
  case Empty
}

