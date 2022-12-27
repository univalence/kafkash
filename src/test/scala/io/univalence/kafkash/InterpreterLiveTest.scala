package io.univalence.kafkash

import org.scalatest.funsuite.AnyFunSuiteLike

class InterpreterLiveTest extends AnyFunSuiteLike {

  test("should display hex in one line") {
    val data   = "0123456789ABCDEF".getBytes()
    val result = InterpreterLive.toHexDump(data)

    assert(result == "0000   30 31 32 33 | 34 35 36 37 | 38 39 41 42 | 43 44 45 46      0123456789ABCDEF")
  }

  test("should display hex with a last partial line") {
    val data   = "0123456789ABCDEF012345".getBytes()
    val result = InterpreterLive.toHexDump(data)

    assert(
      result == "0000   30 31 32 33 | 34 35 36 37 | 38 39 41 42 | 43 44 45 46      0123456789ABCDEF\n0001   30 31 32 33 | 34 35                                        012345"
    )
  }

}
