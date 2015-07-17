package com.mediative.sparrow

import scala.util.control.NonFatal
import scala.math.BigDecimal

import scalaz._
import scalaz.syntax.validation._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter

import Alias._

case class NamedStruct(name: String, tpe: StructType) {
  def index = tpe.fieldNames.indexOf(name)
  def field = tpe.fields.lift(index) getOrElse {
    sys.error(
      s"Cannot find field '$name' in fields: ${tpe.fields.toList}" +
        s"(field names: ${tpe.fieldNames.toList}, index: $index)")
  }

  def description: String = s"$name ($field)"
  def nullCheck(row: Row): Unit = {
    if (row.isNullAt(index))
      throw new NullPointerException(s"The field $description is missing.")
  }
}

trait FieldConverter[T] extends (NamedStruct => V[Row => T]) with Serializable { self =>
  def isNullable: Boolean = false

  def map[U](f: T => U) = new FieldConverter[U] {
    override def isNullable = self.isNullable
    override def apply(struct: NamedStruct): V[Row => U] =
      self.apply(struct).map { _ andThen f }
  }
}

object FieldConverter {

  def convert[A: FieldConverter, B](f: A => B) = reader[A].map(f)

  def reader[T](implicit fc: FieldConverter[T]): FieldConverter[T] = fc

  def simple[T](tpe: DataType, f: (Row, Int) => T): FieldConverter[T] = new FieldConverter[T] {
    override def apply(struct: NamedStruct): V[Row => T] = {
      val index = struct.index
      val field = struct.field
      if (field.dataType != tpe)
        s"The field '${struct.name}' isn't a $tpe as expected, ${field.dataType} received.".failureNel
      else Success { row =>
        struct.nullCheck(row)
        try f(row, index)
        catch {
          case NonFatal(e) =>
            throw new RuntimeException(s"Failed to read the field ${struct.description}).", e)
        }
      }
    }
  }

  implicit def stringConverter: FieldConverter[String] = FieldConverter.simple(StringType, _.getString(_))
  implicit def intConverter: FieldConverter[Int] = FieldConverter.simple(IntegerType, _.getInt(_))
  implicit def longConverter: FieldConverter[Long] = FieldConverter.simple(LongType, _.getLong(_))
  implicit def doubleConverter: FieldConverter[Double] = FieldConverter.simple(DoubleType, _.getDouble(_))
  implicit def bigDecimalConverter: FieldConverter[BigDecimal] = FieldConverter.simple(DecimalType.Unlimited, _.getDecimal(_))
  implicit def bigIntConverter: FieldConverter[BigInt] = FieldConverter.reader[BigDecimal].map(_.toBigInt)

  implicit def localDateConverter: FieldConverter[LocalDate] = stringConverter.map(LocalDate.parse)
  implicit def dateTimeConverter: FieldConverter[DateTime] = stringConverter.map(DateTime.parse)
  implicit def dateTimeConverterFromString(pattern: String): FieldConverter[DateTime] = DatePattern(pattern)
  implicit def dateTimeConverterFromFmt(fmt: DateTimeFormatter): FieldConverter[DateTime] = DatePattern(fmt)
  implicit def localDateConverterFromString(pattern: String): FieldConverter[LocalDate] = DatePattern(pattern)
  implicit def localDateConverterFromFmt(fmt: DateTimeFormatter): FieldConverter[LocalDate] = DatePattern(fmt)

  import java.sql.Timestamp
  implicit def timestampConverter: FieldConverter[Timestamp] = longConverter.map(new Timestamp(_))

  implicit def optionConverter[T](implicit fc: FieldConverter[T]): FieldConverter[Option[T]] =
    new FieldConverter[Option[T]] {
      override def isNullable: Boolean = true
      override def apply(struct: NamedStruct): V[Row => Option[T]] = {
        import struct.index
        if (index == -1) Success(row => None)
        else fc(struct) map { f => row => Some(row).filterNot(_.isNullAt(index)).map(f) }
      }
    }

  implicit def fieldConverter[T](implicit rc: RowConverter[T]): FieldConverter[T] =
    new FieldConverter[T] {
      override def apply(struct: NamedStruct): V[Row => T] = {
        import struct.index
        val dt = struct.field.dataType
        dt match {
          case tpe: StructType =>
            rc.validateAndApply(tpe) map { f =>
              row =>
                struct.nullCheck(row)
                f(row.getAs[Row](index))
            }
          case _ => s"StructType expected, received: $dt".failureNel
        }
      }
    }

  implicit def dateTimeFieldConverter(x: UnixTimestamp.type): FieldConverter[DateTime] =
    FieldConverter.longConverter.map { seconds =>
      new DateTime(seconds * 1000)
    }

  implicit def dateTimeFieldConverter(x: JavaTimestamp.type): FieldConverter[DateTime] =
    FieldConverter.longConverter.map { millis =>
      new DateTime(millis)
    }
}

case object UnixTimestamp
case object JavaTimestamp

case class DatePattern(fmt: DateTimeFormatter)

object DatePattern {
  def apply(pattern: String): DatePattern = DatePattern(DateTimeFormat.forPattern(pattern))

  implicit def toDateTimeFieldConverter(dtp: DatePattern): FieldConverter[DateTime] = {
    FieldConverter.stringConverter.map(dtp.fmt.parseDateTime)
  }

  implicit def toLocalDateFieldConverter(dtp: DatePattern): FieldConverter[LocalDate] = {
    FieldConverter.stringConverter.map(dtp.fmt.parseLocalDate)
  }
}
