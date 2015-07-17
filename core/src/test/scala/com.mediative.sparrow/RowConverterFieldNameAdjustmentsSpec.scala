package com.mediative.sparrow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._

import RowConverter._
import RowConverter.syntax._

import scalaz.{ Failure, Success }

class RowConverterFieldNameAdjustmentsSpec extends FreeSpec {

  import ConverterTester._

  case class Simple(name: String, twoWords: Long)

  object Simple {
    implicit val schema: RowConverter[Simple] = (
      field[String]("name", lenientEqual) and
      field[Long]("twoWords", lenientEqual)
    )(apply _)

    implicit val tpe: Tpe[Simple] = StructType(List(
      StructField("Name", StringType, nullable = false),
      StructField("two_words", LongType, nullable = false)
    ))
  }

  case class SimpleLenient(name: String, twoWords: Long)

  object SimpleLenient {
    implicit val schema: RowConverter[SimpleLenient] = DataFrameReader.createSchema(lenientEqual)

    implicit val tpe: Tpe[SimpleLenient] = StructType(List(
      StructField("Name", StringType, nullable = false),
      StructField("two_words", LongType, nullable = false)
    ))
  }

  case class SimplePartialFunction(name: String, count: Long)

  object SimplePartialFunction {

    private val opt: PartialFunction[String, String] = {
      case "name" => "ID"
    }
    implicit val schema: RowConverter[SimplePartialFunction] = DataFrameReader.createSchema(opt)

    implicit val tpe: Tpe[SimplePartialFunction] = StructType(List(
      StructField("ID", StringType, nullable = false),
      StructField("count", LongType, nullable = false)
    ))
  }

  "lenient equal" - {
    "should tolerate small differences in field names" in {
      test(Row("Hello", 3L), Simple("Hello", 3))
    }

    "should be usable by macros" in {
      test(Row("Hello", 3L), SimpleLenient("Hello", 3))
    }
  }

  "name transformer" - {
    "should allow to use a different name for the case class than the JSON file" in {
      test(Row("Hello", 3L), SimplePartialFunction("Hello", 3))
    }
  }

  case class SimpleFieldOption(@fieldName("description") name: String, id: Long)

  object SimpleFieldOption {
    implicit val schema: RowConverter[SimpleFieldOption] = DataFrameReader.createSchema

    implicit val tpe: Tpe[SimpleFieldOption] = StructType(List(
      StructField("description", StringType, nullable = false),
      StructField("id", LongType, nullable = false)
    ))
  }

  case class SimpleFieldOptionOuter(id: Long, @embedded("Inner") inner: SimpleFieldOptionInner)
  case class SimpleFieldOptionInner(@fieldName("") name: String, id: Long)

  object SimpleFieldOptionOuter {
    implicit val schema: RowConverter[SimpleFieldOptionOuter] = DataFrameReader.createSchema(lenientEqual)

    implicit val tpe: Tpe[SimpleFieldOptionOuter] = StructType(List(
      StructField("id", LongType, nullable = false),
      StructField("Inner", StringType, nullable = false),
      StructField("Inner_ID", LongType, nullable = false)
    ))
  }

  object SimpleFieldOptionInner {
    implicit val schema: RowConverter[SimpleFieldOptionInner] = DataFrameReader.createSchema(lenientEqual)

    implicit val tpe: Tpe[SimpleFieldOptionInner] = StructType(List(
      StructField("", StringType, nullable = false),
      StructField("ID", LongType, nullable = false)
    ))
  }

  "field annotation" - {
    "should allow to rename the field" in {
      test(Row("Hello", 3L), SimpleFieldOption("Hello", 3))
    }

    "should allow to use empty field name" in {
      test(Row("Hello", 3L), SimpleFieldOptionInner("Hello", 3))
    }

    "should allow to use empty field name for embedded fields" in {
      test(Row(42L, "Hello", 3L), SimpleFieldOptionOuter(42, SimpleFieldOptionInner("Hello", 3)))
    }
  }
}
