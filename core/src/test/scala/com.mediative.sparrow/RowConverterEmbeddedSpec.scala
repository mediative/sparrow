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

import scalaz.{ Failure, Success }

import com.github.nscala_time.time.Imports._

class RowConverterEmbeddedSpec extends FreeSpec {

  trait Tpe[T] extends (() => StructType)
  implicit def toTpe[T](tpe: StructType): Tpe[T] = new Tpe[T] {
    def apply() = tpe
  }

  case class Advertiser(
    name: String,
    currency: String,
    id: Long,
    status: String)

  case class Creative(
    name: String,
    id: Option[Long],
    integrationCode: Long,
    source: String,
    status: String)

  case class ReportRow(
    @embedded("Advertiser") advertiser: Advertiser,
    clicks: Long,
    @embedded("Creative") creative: Creative,
    date: LocalDate,
    dfaPlacementId: Option[Long])

  object ReportRow {

    implicit val schema: RowConverter[ReportRow] = DataFrameReader.createSchema(opts)
    private def opts = SchemaOptions(RowConverter.lenientEqual) {
      case "Creativename" => "Creative"
      case "Advertisername" => "Advertiser"
    }

    implicit val tpe: Tpe[ReportRow] = StructType(List(
      StructField("Advertiser", StringType, nullable = false),
      StructField("Advertiser_Currency", StringType, nullable = false),
      StructField("Advertiser_ID", LongType, nullable = false),
      StructField("Advertiser_Status", StringType, nullable = false),
      StructField("Clicks", LongType, nullable = false),
      StructField("Creative", StringType, nullable = false),
      StructField("Creative_ID", LongType, nullable = false),
      StructField("Creative_Integration_Code", LongType, nullable = false),
      StructField("Creative_Source", StringType, nullable = false),
      StructField("Creative_Status", StringType, nullable = false),
      StructField("DATE", StringType, nullable = false),
      StructField("DFA_Placement_ID", LongType, nullable = false)
    ))
  }

  def test[T](row: Row, expected: T)(implicit schema: RowConverter[T], tpe: Tpe[T]) = {
    schema.validateAndApply(tpe()) match {
      case Success(f) => assert(f(row) == expected)
      case Failure(errors) => fail(errors.stream.mkString(". "))
    }
  }

  "@embedded" - {
    "allow to transform a flat structure to a DAG" in {

      val expected = ReportRow(
        Advertiser("Hello", "CAD", 123L, "ACTIVE"),
        123514L,
        Creative("Creative Name", None, 13L, "Source!", "ACTIVE"),
        new LocalDate(2014, 10, 14),
        Some(124L)
      )
      val row = Row(
        "Hello", "CAD", 123L, "ACTIVE", 123514L,
        "Creative Name", null, 13L, "Source!", "ACTIVE",
        "2014-10-14", 124L
      )
      test(row, expected)
    }
  }
}
