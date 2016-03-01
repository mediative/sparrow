/*
 * Copyright 2016 Mediative
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
