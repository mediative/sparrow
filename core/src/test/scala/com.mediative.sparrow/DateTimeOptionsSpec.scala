package com.mediative.sparrow

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._

import com.github.nscala_time.time.Imports._

import RowConverter._
import RowConverter.syntax._

class DateTimeOptionsSpec extends FreeSpec {

  import ConverterTester._

  case class DateTimeHolder(
    name: String,
    dateTime: DateTime)

  object DateTimeHolder {
    implicit val schema = (
      field[String]("name") and
      field[DateTime]("dateTime")(DatePattern("dd/MM/yyyy HH:mm:ss"))
    )(apply _)

    implicit val tpe: Tpe[DateTimeHolder] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("dateTime", StringType, nullable = false)
    ))
  }

  case class LocalDateHolder(
    name: String,
    dateTime: LocalDate)

  object LocalDateHolder {
    implicit val schema = (
      field[String]("name") and
      field[LocalDate]("dateTime")(DatePattern("dd/MM/yyyy"))
    )(apply _)

    implicit val tpe: Tpe[LocalDateHolder] = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("dateTime", StringType, nullable = false)
    ))
  }

  "DateTimeRowConverter" - {
    "should allow define a custom date format for DateTime fields" in {
      test(Row("Hello", "25/12/2015 14:40:00"), DateTimeHolder("Hello", DateTime.parse("2015-12-25T14:40:00.00")))
    }
    "should throw an exception if the DateTime value doesn't have the correct format" in {
      val ex = intercept[IllegalArgumentException] {
        test(Row("Hello", "2/212/2015 14:40:00"), DateTimeHolder("Hello", DateTime.parse("2015-12-25T14:40:00.00")))
      }
      assert(ex.getMessage === "Invalid format: \"2/212/2015 14:40:00\" is malformed at \"2/2015 14:40:00\"")
    }

    "should allow define a custom date format for LocalDate fields" in {
      test(Row("Hello", "25/12/2015"), LocalDateHolder("Hello", LocalDate.parse("2015-12-25")))
    }
  }
}
