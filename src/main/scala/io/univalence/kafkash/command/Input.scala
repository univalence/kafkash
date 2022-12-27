package io.univalence.kafkash.command

/**
 * Represent a flow of data to parse.
 *
 * This type is generified. So, it can be used with Strings, but also
 * with any kind of sequences.
 *
 * @param data sequence to iterate on
 * @param offset offset in the sequence
 */
case class Input[A](data: Seq[A], offset: Int = 0) {
  def hasNext: Boolean = offset < data.length

  def current: A = data(offset)

  def next: Input[A] = copy(offset = offset + 1)
}
object Input {
  type StringInput = Input[Char]
}
