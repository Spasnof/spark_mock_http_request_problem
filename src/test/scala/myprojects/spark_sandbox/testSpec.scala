package myprojects.spark_sandbox

import myprojects.spark_sandbox.test.plusOne
import org.scalatest.flatspec._
import org.scalatest.matchers._


class testSpec extends AnyFlatSpec with should.Matchers {

  "plusOne" should "add one to any int" in {
    val input = 1
    val expected = 2
    val output = plusOne(input)
    output should be(expected)
  }

}
