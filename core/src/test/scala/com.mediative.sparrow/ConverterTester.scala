package com.mediative.sparrow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.Assertions.fail
import org.scalatest.Matchers

import scalaz.{ Failure, Success }

object ConverterTester extends Matchers {

  trait Tpe[T] extends (() => StructType)
  implicit def toTpe[T](tpe: StructType): Tpe[T] = new Tpe[T] {
    def apply() = tpe
  }

  def test[T](row: Row, expected: T)(implicit schema: RowConverter[T], tpe: Tpe[T]) = {
    schema.validateAndApply(tpe()) match {
      case Success(f) => assert(f(row) == expected)
      case Failure(errors) => fail(errors.stream.mkString(". "))
    }
  }
}
