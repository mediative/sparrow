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

import org.scalatest._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * Reusable base class for codec limitation specs.
 */
trait CodecLimitationsTestBase extends FreeSpec with BeforeAndAfterAll {

   val sc = new SparkContext("local", "test2")
   val sqlContext = new SQLContext(sc)

   override def afterAll() = sc.stop()

   // To get DataFrame#toRDD usage.
   import com.mediative.sparrow.syntax.df._
   import scala.reflect.runtime.universe.TypeTag

   def assertCodec[T <: Product: ClassTag: TypeTag: RowConverter](value: T): Unit = {
     val rdd0 = sc.parallelize(List(value))
     assertResult(1) { rdd0.count }
     val df = sqlContext.createDataFrame(rdd0)
     val rdd1Maybe = df.toRDD[T]
     assert(rdd1Maybe.isSuccess, rdd1Maybe)
     val rdd1 = rdd1Maybe.toOption.get
     assertResult(0) { rdd0.subtract(rdd1).count }
     assertResult(0) { rdd1.subtract(rdd0).count }
   }
}

/**
 * By design, toRDD requires the classes it works on to take at least two
 * public constructor arguments.
 */
object CodecLimitationsTest {
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

/**
 * Specifications to track current limitations related with marshalling.
 *
 * By design, toRDD requires the classes it works on to take at least two
 * public constructor arguments.
 */
class CodecLimitationsTest extends CodecLimitationsTestBase {
  import CodecLimitationsTest._

  "toRDD should" - {

    import DataFrameReader._

    "successfully marshall RDD => DataFrame => RDD an object containing" - {
      "Int, String" in {
        assertCodec(TestToRdd1(1, "a"))
      }

      "Int, Option[Int]" - {
        "when Some(Int)" in {
          assertCodec(TestToRdd2(1, Option(1)))
        }

        "when None" in {
          assertCodec(TestToRdd2(1, Option.empty))
        }
      }

      "String, java.sql.Timestamp" in {
        pendingUntilFixed {
          // FIXME:
          // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'timestampVal' isn't a LongType as expected, TimestampType received.)) (DataFrameReaderTest.scala:203)
          assertCodec(
            TestToRdd3("a", Timestamp.valueOf("2015-07-15 09:00:00"))
          )
        }
      }

      "Int, Option[Double]" - {
        "when Some(Double)" in {
          assertCodec(TestToRdd5(1, Some(2.0)))
        }
        "when None" in {
          assertCodec(TestToRdd5(1, None))
        }
      }

      "Int, Wrapped[String]" in {
        pendingUntilFixed {
          // FIXME:
          // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedStringVal' isn't a StringType as expected, StructType(StructField(unwrap,StringType,true)) received.)) (DataFrameReaderTest.scala:207)
          assertCodec(TestToRdd8(1, Wrap.Wrapped("foo")))
        }
      }

      "Int, Option[Wrapped[Double]]" - {
        "when None" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedDoubleVal' isn't a DoubleType as expected, StructType(StructField(unwrap,DoubleType,false)) received.)) (DataFrameReaderTest.scala:207)
            assertCodec(TestToRdd6(1, None))
          }
        }
        "when Some(Wrapped[Double])" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedDoubleVal' isn't a DoubleType as expected, StructType(StructField(unwrap,DoubleType,false)) received.)) (DataFrameReaderTest.scala:204)
            assertCodec(TestToRdd6(1, Some(Wrap.Wrapped(2.0))))
          }
        }
      }

      "Int, Option[Wrapped[String]]" - {
        "when None" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedStringVal' isn't a StringType as expected, StructType(StructField(unwrap,StringType,true)) received.)) (DataFrameReaderTest.scala:204)
            assertCodec(TestToRdd7(1, None))
          }
        }
        "when Some(Wrapped[String])" in {
          pendingUntilFixed {
            // FIXME:
            // rdd1Maybe.isSuccess was false Failure(NonEmptyList(The field 'wrappedStringVal' isn't a StringType as expected, StructType(StructField(unwrap,StringType,true)) received.)) (DataFrameReaderTest.scala:204)
            assertCodec(TestToRdd7(1, Some(Wrap.Wrapped("foo"))))
          }
        }
      }
    }
  }
}
