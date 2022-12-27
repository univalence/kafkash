package io.univalence.kafkash.command

import zio.test.*

object InputSpec extends ZIOSpecDefault {
  override def spec =
    suite("Input spec")(
      test("should get current element") {
        val input = Input("12345")

        assertTrue(input.current == '1')
      },
      test("should get next input") {
        val input = Input("12345").next

        assertTrue(input == Input("12345", 1))
      },
      test("should get next input even at the end of input") {
        val input = Input("12345", 6).next

        assertTrue(input == Input("12345", 7))
      },
      test("should get true when has next") {
        val input = Input("12345")

        assertTrue(input.hasNext)
      },
      test("should get false when has no next") {
        val input = Input("12345", 6)

        assertTrue(!input.hasNext)
      },
      test("should input a list") {
        val input = Input(List("123", "456", "789"))

        assertTrue(input.current == "123")
      }
    )
}
