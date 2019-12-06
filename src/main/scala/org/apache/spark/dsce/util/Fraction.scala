package org.apache.spark.dsce.util

class Fraction(c: BigInt, d: BigInt) {
  val divisor = c.gcd(d);
  def counter = c / divisor;
  def denominator = d / divisor;

  def lcm(a: BigInt, b: BigInt): BigInt = (a * b).abs / a.gcd(b)

  def toDouble = (counter.doubleValue() / denominator.doubleValue())

  def +(f: Fraction): Fraction = {
    val lcmVal = lcm(f.denominator, denominator)
    val apply = (count: BigInt, div: BigInt) => lcmVal / div * count
    new Fraction(
      apply(f.counter, f.denominator)
        + apply(counter, denominator),
      lcmVal
    )
  }

  def *(f: Fraction): Fraction = {
    new Fraction(counter * f.counter, denominator * f.denominator)
  }

  override def toString(): String = {
    String.format("%s/%s", counter, denominator)
  }
}

object Fraction {
  def apply(c: BigInt, d: BigInt) = new Fraction(c, d)
}
