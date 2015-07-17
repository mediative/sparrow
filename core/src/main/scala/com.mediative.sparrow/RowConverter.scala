package com.mediative.sparrow

import scalaz._
import Scalaz.ToApplyOps
import scalaz.syntax.validation._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import play.api.libs.functional.{ Applicative => PApplicative, Functor => PFunctor, FunctionalBuilderOps }

import Alias._

trait RowConverter[T] extends (StructType => V[Row => T]) with Serializable { self =>
  def validateFields(fields: Set[String]): (V[Unit], Set[String])

  def validateStruct(schema: StructType): V[Unit] = {
    val (v, others) = validateFields(schema.fieldNames.toSet)
    val extraFields =
      if (others.isEmpty) ().success
      else s"There are extra fields: $others".failureNel

    (v |@| extraFields) { (_, _) => () }
  }

  def map[U](f: T => U): RowConverter[U] = new RowConverter[U] {
    override def validateFields(fields: Set[String]) = self.validateFields(fields)
    override def apply(tpe: StructType): V[Row => U] = {
      for {
        g <- self(tpe)
      } yield {
        g andThen f
      }
    }
  }

  def validateAndApply(tpe: StructType): V[Row => T] = {
    import scalaz.Validation.FlatMap._
    validateStruct(tpe) flatMap { _ =>
      apply(tpe)
    }
  }
}

object RowConverter {

  object syntax {
    import play.api.libs.functional.syntax.functionalCanBuildApplicative

    implicit def toFunctionalBuilderOps[A](a: RowConverter[A]): FunctionalBuilderOps[RowConverter, A] = {
      val cbf = functionalCanBuildApplicative(RowConverterApplicative)
      play.api.libs.functional.syntax.toFunctionalBuilderOps(a)(cbf)
    }
  }

  implicit object RowConverterApplicative extends PApplicative[RowConverter] with PFunctor[RowConverter] {
    def pure[A](a: A): RowConverter[A] = new RowConverter[A] {
      override def validateFields(fields: Set[String]) = (().success, fields)
      override def apply(tpe: StructType) = Success(_ => a)
    }

    def fmap[A, B](m: RowConverter[A], f: A => B): RowConverter[B] = map(m, f)
    def map[A, B](m: RowConverter[A], f: A => B): RowConverter[B] = m.map(f)

    def apply[A, B](mf: RowConverter[A => B], ma: RowConverter[A]): RowConverter[B] = new RowConverter[B] {
      override def validateFields(fields: Set[String]) = {
        val (v1, fs1) = mf.validateFields(fields)
        val (v2, fs2) = ma.validateFields(fs1)
        (v1 |@| v2)((_, _) => ()) -> fs2
      }
      override def apply(tpe: StructType): V[Row => B] = {
        (ma(tpe) |@| mf(tpe)) { (ra, rab) =>
          (row: Row) => rab(row)(ra(row))
        }
      }
    }
  }

  val lenientEqual: Equal[String] = {
    def normalize(s: String) = s.replaceAllLiterally("_", "").toLowerCase
    Equal.equal { (a, b) =>
      normalize(a) == normalize(b)
    }
  }

  def field[T](name: String, equal: Equal[String] = Equal.equalA)(implicit fc: FieldConverter[T]): RowConverter[T] =
    new RowConverter[T] {
      override def validateFields(fields: Set[String]): (V[Unit], Set[String]) = {
        val (named, others) = fields.partition(equal.equal(_, name))

        val v =
          if (named.isEmpty && !fc.isNullable) s"The field '$name' is missing".failureNel
          else ().success
        v -> others
      }

      override def apply(tpe: StructType): V[Row => T] = {
        val fieldName = tpe.fieldNames.find(equal.equal(name, _)) getOrElse {
          if (fc.isNullable) name
          else sys.error(
            s"""
               |Assertion failure, the field should have been validated to exist.
               |Field name: $name, StrucType: $tpe.
               |""".stripMargin)
        }
        fc(NamedStruct(fieldName, tpe))
      }
    }
}
