package io.univalence.kafkash

import org.scalatest.funsuite.AnyFunSuiteLike

class KafkaShellMainTest extends AnyFunSuiteLike {

  test("should have no parameter when no args") {
    val (parameters, values) = KafkaShellMain.getParameters(Array())

    assert(parameters.isEmpty)
    assert(values.isEmpty)
  }

  test("should set a single parameter") {
    val (parameters, values) = KafkaShellMain.getParameters(Array("--bootstrap.servers", "1.1.1.1:9092"))

    assert(parameters === Map("bootstrap.servers" -> List("1.1.1.1:9092")))
    assert(values.isEmpty)
  }

  test("should set a single parameter with =") {
    val (parameters, values) = KafkaShellMain.getParameters(Array("--bootstrap.servers=1.1.1.1:9092"))

    assert(parameters === Map("bootstrap.servers" -> List("1.1.1.1:9092")))
    assert(values.isEmpty)
  }

  test("should set a single parameter with no value") {
    val (parameters, values) = KafkaShellMain.getParameters(Array("--help"))

    assert(parameters === Map("help" -> List()))
    assert(values.isEmpty)
  }

}
