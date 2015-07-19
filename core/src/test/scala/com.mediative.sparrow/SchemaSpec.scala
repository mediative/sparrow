/*
 * Copyright 2015 Mediative
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
import org.apache.spark.sql.types._
import org.scalatest._
import ConverterTester._

import com.github.nscala_time.time.Imports._

object SchemaSpec {

  @schema
  case class WithoutCompanion(name: String, count: Long)

  implicit val tpe: Tpe[WithoutCompanion] = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("count", LongType, nullable = false)
  ))

  @schema
  case class WithCompanion(name: String, count: Long)

  object WithCompanion {
    implicit val tpeWithCompanion: Tpe[WithCompanion] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("count", LongType, nullable = false)
    ))
  }

  @schema
  case class WithFieldOptions(
    @fieldName("NAME") name: String,
    count: Long)

  implicit val tpeWithFieldOptions: Tpe[WithFieldOptions] = StructType(List(
    StructField("NAME", StringType, nullable = false),
    StructField("count", LongType, nullable = false)
  ))

  @schema
  case class WithBody(name: String, count: Long) {
    override def toString = name
    def description = s"$name($count)"
  }

  object WithBody {
    implicit val tpe: Tpe[WithBody] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("count", LongType, nullable = false)
    ))
    // FIXME the create methods should be named apply,
    // but the (apply _) synthax used by the macro
    // doesn't support apply to be overloaded,
    // so the function should be called directly
    // in the generated code instead of
    // use partial application.
    def create: WithBody = create("World")
    def create(name: String): WithBody = WithBody(name, 10)
  }

  @schema
  case class WithSingleStatement(name: String, count: Long) {
    def description = s"$name($count)"
  }

  object WithSingleStatement {
    implicit val tpe: Tpe[WithSingleStatement] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("count", LongType, nullable = false)
    ))
  }

  @schema(equal = RowConverter.lenientEqual)
  case class Lenient(name: String, count: Long)

  object Lenient {
    implicit val tpe: Tpe[Lenient] = StructType(List(
      StructField("NAME", StringType, nullable = false),
      StructField("Count", LongType, nullable = false)
    ))
  }

  @schema({
    case "name" => "NAME"
  })
  case class WithPF(name: String, count: Long)

  object WithPF {
    implicit val tpe: Tpe[WithPF] = StructType(List(
      StructField("NAME", StringType, nullable = false),
      StructField("count", LongType, nullable = false)
    ))
  }

  @schema({ case "name" => "id" }, equal = RowConverter.lenientEqual)
  case class WithSchemaOptions(name: String, count: Long)

  object WithSchemaOptions {
    implicit val tpe: Tpe[WithSchemaOptions] = StructType(List(
      StructField("ID", StringType, nullable = false),
      StructField("Count", LongType, nullable = false)
    ))
  }

  case class EmbeddedChild(@fieldName("") name: String, count: Long)

  @schema(equal = RowConverter.lenientEqual)
  case class Parent(name: String, @embedded(prefix = "Child") child: EmbeddedChild)

  object Parent {
    implicit val tpe: Tpe[Parent] = StructType(List(
      StructField("Name", StringType, nullable = false),
      StructField("Child", StringType, nullable = false),
      StructField("Child_Count", LongType, nullable = false)
    ))
  }

  @schema
  case class DateTimeHolder(
    name: String,
    @fieldOption(DatePattern("dd/MM/yyyy HH:mm:ss")) dateTime: DateTime)

  object DateTimeHolder {
    implicit val tpe: Tpe[DateTimeHolder] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("dateTime", StringType, nullable = false)
    ))
  }

  @schema
  case class LocalDateHolder(
    name: String,
    @fieldOption("dd/MM/yyyy") dateTime: LocalDate)

  object LocalDateHolder {
    implicit val tpe: Tpe[LocalDateHolder] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("dateTime", StringType, nullable = false)
    ))
  }

  @schema
  case class UnixDateHolder(
    name: String,
    @fieldOption(UnixTimestamp) dateTime: DateTime)

  object UnixDateHolder {
    implicit val tpe: Tpe[UnixDateHolder] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("dateTime", LongType, nullable = false)
    ))
  }

  @schema
  case class JavaTimestampHolder(
    name: String,
    @fieldOption(JavaTimestamp) dateTime: DateTime)

  object JavaTimestampHolder {
    implicit val tpe: Tpe[JavaTimestampHolder] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("dateTime", LongType, nullable = false)
    ))
  }
}

class SchemaSpec extends FreeSpec {

  import SchemaSpec._

  "@schema" - {
    "should generate an implicit schema in an existing companion object" in {
      test(Row("Hello", 1L), WithCompanion("Hello", 1))
    }

    "should generate an implicit schema in a new companion object" in {
      test(Row("Hello", 1L), WithoutCompanion("Hello", 1))
    }

    "should support @fieldOption name" in {
      test(Row("Hello", 1L), WithFieldOptions("Hello", 1))
    }

    "should support case class with multiple statements in the body" in {
      // Making sure that no method was removed from WithBody
      assert(WithBody.create.description == "World(10)")
      test(Row("Hello", 1L), WithBody("Hello", 1))
    }

    "should support case class with a single statement in the body" in {
      test(Row("Hello", 1L), WithSingleStatement("Hello", 1))
    }

    "should support lenient equal" in {
      test(Row("Hello", 1L), Lenient("Hello", 1))
    }

    "should support partial function rename" in {
      test(Row("Hello", 1L), WithPF("Hello", 1))
    }

    "should support both lenient equal and partial function rename on the same case class" in {
      test(Row("Hello", 1L), WithSchemaOptions("Hello", 1))
    }

    "should support @embedded" in {
      test(Row("Hello", "World", 1L), Parent("Hello", EmbeddedChild("World", 1)))
    }

    "should support @fieldOption with DatePattern as option" in {
      test(Row("Hello", "25/12/2015 14:40:00"), DateTimeHolder("Hello", DateTime.parse("2015-12-25T14:40:00.00")))
    }
    "should support @fieldOption with a string as option" in {
      test(Row("Hello", "25/12/2015"), LocalDateHolder("Hello", LocalDate.parse("2015-12-25")))
    }
    "should support @fieldOption with a unix timestamp" in {
      val seconds = System.currentTimeMillis / 1000
      test(Row("Hello", seconds), UnixDateHolder("Hello", new DateTime(seconds * 1000)))
    }
    "should support @fieldOption with a java timestamp" in {
      val now = System.currentTimeMillis
      test(Row("Hello", now), JavaTimestampHolder("Hello", new DateTime(now)))
    }
  }
}
