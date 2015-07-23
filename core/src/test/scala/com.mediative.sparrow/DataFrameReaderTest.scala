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

import java.sql.Timestamp
import scala.reflect.ClassTag

import scalaz._
import scalaz.syntax.validation._

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import play.api.libs.functional.syntax._

import RowConverter._
import Alias._

object DataFrameReaderTest {
  case class Simple(name: String, count: Long)

  object Simple {
    implicit val schema = (
      field[String]("name") and
      field[Long]("count")
    )(apply _)
  }

  case class WithSimpleOption(name: String, count: Long, description: Option[String])

  object WithSimpleOption {
    implicit val schema = (
      field[String]("name") and
      field[Long]("count") and
      field[Option[String]]("description")
    )(apply _)
  }

  case class WithNested(name: String, inner: Simple, innerOpt: Option[WithSimpleOption])

  object WithNested {
    implicit val schema = (
      field[String]("name") and
      field[Simple]("inner") and
      field[Option[WithSimpleOption]]("innerOpt")
    )(apply _)
  }

  case class SimpleMap(name: String, count: Int)

  object SimpleMap {
    implicit val schema = (
      field[String]("name") and
      field[String]("count").map(_.toInt)
    )(apply _)
  }

  sealed abstract class PetType
  case object Dog extends PetType
  case object Cat extends PetType
  case object Hamster extends PetType

  object PetType {
    implicit val schema: FieldConverter[PetType] =
      FieldConverter.reader[String].map {
        case "dog" => Dog
        case "cat" => Cat
        case "hamster" => Hamster
      }
  }

  case class Pet(name: String, tpe: PetType)

  object Pet {
    implicit val schema = (
      field[String]("name") and
      field[PetType]("type")
    )(apply _)
  }
}

/*
By design, toRDD requires the classes it works on to take at least two
public constructor arguments.
*/
object TestCaseClasses {
  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd1(intVal: Int, stringVal: String)

  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd2(intVal: Int, intOptionVal: Option[Int])

  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd3(stringVal: String, timestampVal: Timestamp)

  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd4(intVal: Int, doubleVal: Double)

  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd5(intVal: Int, doubleVal: Option[Double])

  import Wrap._
  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd6(intVal: Int, wrappedDoubleVal: Option[Wrapped[Double]])

  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd7(intVal: Int, wrappedStringVal: Option[Wrapped[String]])

  @schema(equal = RowConverter.lenientEqual)
  case class TestToRdd8(intVal: Int, wrappedStringVal: Wrapped[String])

  object Wrap {
    case class Wrapped[T](unwrap: T)
    implicit def wrappedDoubleConverter[T: FieldConverter]: FieldConverter[Wrapped[T]] =
      FieldConverter.reader[T].map(Wrapped(_))
  }
}

class DataFrameReaderTest extends FreeSpec with BeforeAndAfterAll {
  import DataFrameReaderTest._

  val sc = new SparkContext("local", "test2")
  val sqlContext = new SQLContext(sc)

  override def afterAll() = sc.stop()

  "RowConverter" - {

    def testSerialization(obj: Any) = {
      import java.io._
      val buf = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(buf)
      out.writeObject(obj)
      out.flush()

      val in = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray))
      assert(obj.getClass == in.readObject().getClass)
    }

    "can be serialized" in {
      val simple = StructType(Seq(StructField("name", StringType), StructField("count", LongType)))
      val withNested = StructType(Seq(
        StructField("name", StringType),
        StructField("inner", simple),
        StructField("innerOpt", simple, nullable = true)))

      Simple.schema.validateAndApply(simple) match {
        case Success(f) =>
          testSerialization(f)
          testSerialization(f(Row("Name", 12L)))
        case Failure(e) => fail(e.toString)
      }

      WithNested.schema.validateAndApply(withNested) match {
        case Success(f) =>
          testSerialization(f)
          testSerialization(f(Row("Name", Row("Name", 12L), null)))
        case Failure(e) => fail(e.toString)
      }
    }
  }

  "toRDD should" - {

    import DataFrameReader._

    def testSuccess[T: RowConverter: ClassTag](json: Array[String], expected: List[T]) = {
      val df = sqlContext.jsonRDD(sc.parallelize(json))
      val rdd = toRDD[T](df).valueOr { es => fail((es.head :: es.tail).mkString("\n")) }

      assert(rdd.collect().toList == expected)
    }

    def testFailure[T: RowConverter: ClassTag](json: Array[String], expected: NonEmptyList[String]) = {
      val df = sqlContext.jsonRDD(sc.parallelize(json))

      assert(toRDD[T](df) == expected.failure)
    }

    "successfully marshall RDD => DataFrame => RDD an object containing" - {
      // To get DataFrame#toRDD usage.
      import com.mediative.sparrow.syntax.df._
      import scala.reflect.runtime.universe.TypeTag
      def testRDD_DataFrame_codec[T <: Product: ClassTag: TypeTag: RowConverter](value: T): Unit = {
        val rdd0 = sc.parallelize(List(value))
        assertResult(1) { rdd0.count }
        val df = sqlContext.createDataFrame(rdd0)
        val rdd1Maybe = df.toRDD[T]
        assert(rdd1Maybe.isSuccess, rdd1Maybe)
        val rdd1 = rdd1Maybe.toOption.get
        assertResult(0) { rdd0.subtract(rdd1).count }
        assertResult(0) { rdd1.subtract(rdd0).count }
      }

      import TestCaseClasses._

      "Int, String" in {
        testRDD_DataFrame_codec(TestToRdd1(1, "a"))
      }

      "Int, Option[Int]" - {
        "when Some(Int)" in {
          testRDD_DataFrame_codec(TestToRdd2(1, Option(1)))
        }

        "when None" in {
          testRDD_DataFrame_codec(TestToRdd2(1, Option.empty))
        }
      }

      "String, java.sql.Timestamp" in {
        pendingUntilFixed {
          // FIXME:
          // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'timestampVal' isn't a LongType as expected, TimestampType received.)) (DataFrameReaderTest.scala:203)
          testRDD_DataFrame_codec(
            TestToRdd3("a", Timestamp.valueOf("2015-07-15 09:00:00"))
          )
        }
      }

      "Int, Double" in {
        pendingUntilFixed {
          // FIXME:
          // "org.apache.spark.SparkException: Job aborted due to stage failure"
          // Caused by: java.lang.ClassCastException: java.lang.Double cannot be cast to java.lang.Integer
          // at scala.runtime.BoxesRunTime.unboxToInt(BoxesRunTime.java:106)
          testRDD_DataFrame_codec(TestToRdd4(1, 2.0))
        }
      }

      "Int, Option[Double]" - {
        "when Some(Double)" in {
          testRDD_DataFrame_codec(TestToRdd5(1, Some(2.0)))
        }
        "when None" in {
          testRDD_DataFrame_codec(TestToRdd5(1, None))
        }
      }

      "Int, Wrapped[String]" in {
        pendingUntilFixed {
          // FIXME:
          // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedStringVal' isn't a StringType as expected, StructType(StructField(unwrap,StringType,true)) received.)) (DataFrameReaderTest.scala:207)
          testRDD_DataFrame_codec(TestToRdd8(1, Wrap.Wrapped("foo")))
        }
      }

      "Int, Option[Wrapped[Double]]" - {
        "when None" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedDoubleVal' isn't a DoubleType as expected, StructType(StructField(unwrap,DoubleType,false)) received.)) (DataFrameReaderTest.scala:207)
            testRDD_DataFrame_codec(TestToRdd6(1, None))
          }
        }
        "when Some(Wrapped[Double])" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedDoubleVal' isn't a DoubleType as expected, StructType(StructField(unwrap,DoubleType,false)) received.)) (DataFrameReaderTest.scala:204)
            testRDD_DataFrame_codec(TestToRdd6(1, Some(Wrap.Wrapped(2.0))))
          }
        }
      }

      "Int, Option[Wrapped[String]]" - {
        "when None" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedStringVal' isn't a StringType as expected, StructType(StructField(unwrap,StringType,true)) received.)) (DataFrameReaderTest.scala:204)
            testRDD_DataFrame_codec(TestToRdd7(1, None))
          }
        }
        "when Some(Wrapped[String])" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedStringVal' isn't a StringType as expected, StructType(StructField(unwrap,StringType,true)) received.)) (DataFrameReaderTest.scala:204)
            testRDD_DataFrame_codec(TestToRdd7(1, Some(Wrap.Wrapped("foo"))))
          }
        }
      }
    }

    "work for simple case class with only primitives" in {
      val json = Array(
        """{"name": "First's Inner", "count": 121}""",
        """{"name": "Last's inner", "count": 12}"""
      )
      val expected = List(
        Simple("First's Inner", count = 121),
        Simple("Last's inner", count = 12)
      )

      testSuccess(json, expected)
    }

    "support optional fields" - {
      "when completely missing from the json" in {
        val json = Array(
          """{"name": "First's name", "count": 121}""",
          """{"name": "Last's name", "count": 12}"""
        )
        val expected = List(
          WithSimpleOption("First's name", count = 121, None),
          WithSimpleOption("Last's name", count = 12, None)
        )

        testSuccess(json, expected)
      }
      "when partially present in the json" in {
        val json = Array(
          """{"name": "First's name", "count": 121, "description": "abc"}""",
          """{"name": "Last's name", "count": 12}"""
        )
        val expected = List(
          WithSimpleOption("First's name", count = 121, Some("abc")),
          WithSimpleOption("Last's name", count = 12, None)
        )

        testSuccess(json, expected)
      }
    }

    "support nested objects" in {
      val json = Array(
        """{"name": "Guillaume", "inner": {"name": "First Inner", "count": 121}}""",
        """{"name": "Last", "inner": {"name": "Last Inner", "count": 12}}"""
      )
      val expected = List(
        WithNested("Guillaume", Simple("First Inner", 121), None),
        WithNested("Last", Simple("Last Inner", 12), None)
      )

      testSuccess(json, expected)
    }

    "validate extra fields" in {
      val json = Array(
        """{"name": "Guillaume", "inner": {"name": "First's Inner", "count": 121, "abc": 244}}""",
        """{"name": "Last", "inner": {"name": "Last's inner", "count": 12}}"""
      )

      testFailure[WithNested](json, NonEmptyList.nel("There are extra fields: Set(abc)", Nil))
    }

    "validate mixed type for a field with conversion possible (e.g. same colum has both String and Int)" in {
      val json = Array(
        """{"name": "First's Inner", "count": 121}""",
        """{"name": 2, "count": 12}"""
      )
      val expected = List(
        Simple("First's Inner", count = 121),
        Simple("2", count = 12)
      )

      testSuccess(json, expected)
    }

    "validate mixed type for a field without conversion possible (e.g. same colum has both String and Int)" in {
      val json = Array(
        """{"name": "First's Inner", "count": 121}""",
        """{"name": "Second", "count": "12"}"""
      )
      val expected = List(
        Simple("First's Inner", count = 121),
        Simple("Second", count = 12)
      )

      testFailure[Simple](json, NonEmptyList.nel(
        "The field 'count' isn't a LongType as expected, StringType received.", Nil))
    }

    "work with ADT enums" in {
      val json = Array(
        """{"name": "Chausette", "type": "dog"}""",
        """{"name": "Mixcer", "type": "cat"}"""
      )
      val expected = List(
        Pet("Chausette", Dog),
        Pet("Mixcer", Cat)
      )

      testSuccess(json, expected)
    }
  }
}
