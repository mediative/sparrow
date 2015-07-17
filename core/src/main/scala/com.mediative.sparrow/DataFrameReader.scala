package com.mediative.sparrow

import language.experimental.macros
import reflect.macros.Context
import scala.annotation.StaticAnnotation
import scala.reflect.internal.annotations.compileTimeOnly
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

import scalaz.{ Equal, ValidationNel }

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Alias {
  type V[T] = ValidationNel[String, T]
}
import Alias._

package syntax {
  object df extends ToDataFrameOps
}

trait ToDataFrameOps {
  implicit def ToDataFrameOpsFromDataFrame(a: DataFrame): DataFrameOps = new DataFrameOps(a)
}

final class DataFrameOps(val self: DataFrame) extends AnyVal {
  def toRDD[T: ClassTag: RowConverter]: V[RDD[T]] = {
    DataFrameReader.toRDD[T](self)
  }
}

case class SchemaOptions(
    nameTransformer: PartialFunction[String, String] = PartialFunction.empty,
    equal: Equal[String] = Equal.equalA) {
  def transform(name: String): String = nameTransformer.applyOrElse(name, identity[String])
}

object SchemaOptions {
  implicit def defaultOptions: SchemaOptions = SchemaOptions()
  implicit def fromEqual(equal: Equal[String]): SchemaOptions = SchemaOptions(equal = equal)
  implicit def fromTransformer(transformer: PartialFunction[String, String]): SchemaOptions =
    SchemaOptions(nameTransformer = transformer)

  def apply(equal: Equal[String])(nameTransformer: PartialFunction[String, String]): SchemaOptions =
    SchemaOptions(nameTransformer, equal)

}

class embedded(prefix: String = "") extends StaticAnnotation
class fieldName(name: String) extends StaticAnnotation
class fieldOption(option: Any) extends StaticAnnotation

@compileTimeOnly("This annotation requires macro paradise.")
class schema(
  nameTransformer: PartialFunction[String, String] = PartialFunction.empty,
  equal: Equal[String] = Equal.equalA)
    extends StaticAnnotation {

  def macroTransform(annottees: Any*): Any = macro DataFrameReader.annotationImpl
}

object DataFrameReader {
  def toRDD[T: ClassTag](df: DataFrame)(implicit rc: RowConverter[T]): V[RDD[T]] = {
    rc.validateAndApply(df.schema).map { f => df.map(f) }
  }

  /**
   * This function will use a macro to inspect the case class' AST and generate code similar to:
   *
   * <code>
   *   implicit val schema = (
   *     field[String]("name") and
   *     field[Int]("count")
   *   )(apply _)
   * </code>
   *
   * This macro also support an @embedded annotation that will treat a field as another case class
   * to embed in the parent one instead of relying on its schema. Thus a case class like:
   *
   * <code>
   *   case class Child(firstName: String, lastName: String)
   *   case class Parent(id: Int, @embedded("prefix_") child: Child)
   * </code>
   *
   * The macro will generate code similar to:
   *
   * <code>
   *   implicit val schema = (
   *     field[Int]("name") and
   *     (
   *       field[String]("prefix_firstName") and
   *       field[String]("prefix_lastName")
   *     )(apply _)
   *   )(apply _)
   * </code>
   *
   * The SchemaOption case class also allows further customization related to field names.
   * Taking into account the options, the generated code will be closer to:
   *
   * <code>
   *   implicit val schema = (
   *     field[String](options.transform("name"), options.equal) and
   *     field[Int](options.transform("count"), options.equal)
   *   )(apply _)
   * </code>
   *
   * The instance of Equal will be used to find a match between the case class field name and
   * the DataFrame's field name. The transform function uses the provided partial function
   * to support things like more specific renaming of fields. The unit tests provides examples.
   *
   * @tparam T the type of the case class for which to generate a RowConverter[T] composite
   */
  def createSchema[T](implicit options: SchemaOptions): RowConverter[T] = macro createSchemaImpl[T]

  def createSchemaImpl[T: c.WeakTypeTag](c: Context)(options: c.Expr[SchemaOptions]): c.Expr[RowConverter[T]] = {
    import c.universe._

    val emdeddedType = weakTypeOf[embedded]
    val fieldNameType = weakTypeOf[fieldName]
    val fieldOptionsType = weakTypeOf[fieldOption]

    val optionsDeclaration = q"val options = $options"
    val optionsName = optionsDeclaration match {
      case q"val $name = $value" => name
    }

    def converter(tpe: Type, prefix: Option[Tree]): Tree = {

      val declarations = tpe.declarations
      val ctor = declarations.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m
      } getOrElse {
        val msg = "Cannot find the primary constructor for type " + tpe
        c.abort(c.enclosingPosition, msg)
      }

      val params = ctor.paramss.head
      val fields = params.map { p =>
        val fieldType = tpe.declaration(p.name).typeSignature
        val name = p.name.decoded

        val fieldNameOpt = p.annotations.find(_.tpe == fieldNameType)
        val fieldOptions = p.annotations.find(_.tpe == fieldOptionsType)

        p.annotations.find(_.tpe == emdeddedType).map { at =>
          if (fieldNameOpt.isDefined || fieldOptions.isDefined)
            c.abort(c.enclosingPosition, "@embedded and @fieldName or @fieldOption cannot be used on the same field.")
          converter(fieldType, at.scalaArgs.headOption)
        } getOrElse {
          val p = prefix.getOrElse(q""" "" """)

          val block = fieldNameOpt.map { at =>
            val fieldName = at.scalaArgs.head
            q"field[$fieldType]($p + $fieldName)"
          } getOrElse {
            q"field[$fieldType]($optionsName.transform($p + $name), $optionsName.equal)"
          }

          fieldOptions.fold(block) { fc =>
            q"$block(${fc.scalaArgs.head})"
          }
        }
      }.toList

      //
      // This macro serves no purpose for case class without field, so it will never be supported.
      //
      // However, case classes with only one field are also not supported right now. While there could
      // be some usefulness, there's generally not much value in case class with only one field
      // other than as a wrapper, in which case they should likely be serialized to a string,
      // not to a single field case class.
      //
      // If support is required in the future, it can be implemented. The reason it isn't supported
      // from the current code is the macro would generate something like:
      //
      //    implicit val schema = (
      //      field[String]("name")
      //    )(apply _)
      //
      // The problem with this is that without the `and`, the RowConverter isn't converted to a
      // functional builder and the apply method isn't defined.
      //
      if (fields.size < 2) {
        c.error(c.enclosingPosition, "Only case classes with more than one field are supported.")
      }

      val composite = fields.reduceLeft { (left, right) => q"$left and $right" }
      val companion = tpe.typeSymbol.companionSymbol
      val applies = companion.asModule.typeSignature.members
        .filter(_.name.decoded == "apply")
        .filter(_.isMethod)
      val exists = applies
        .exists { apply =>
          val m = apply.asMethod
          m.returnType.typeSymbol == tpe.typeSymbol && {
            m.paramss match {
              case applyParams :: Nil =>
                fields.length == applyParams.length && {
                  (params zip applyParams) forall {
                    case (x, y) =>
                      x.typeSignature == y.typeSignature
                  }
                }
              case _ => false
            }
          }
        }
      if (!exists) {
        val msg =
          s"""
            | Cannot find an apply method with the proper signature.
            | tpe: $tpe
            | apply methods: $applies
          """.stripMargin
        c.info(c.enclosingPosition, msg, force = true)
        c.error(c.enclosingPosition, "Cannot find an apply method with the proper signature.")
      }

      q"$composite($companion.apply _)"
    }

    val tpe = implicitly[c.WeakTypeTag[T]].tpe

    val code = q"""
      import _root_.com.mediative.sparrow.RowConverter._
      import _root_.com.mediative.sparrow.RowConverter.syntax._
      $optionsDeclaration
      ${converter(tpe, None)}
    """

    c.Expr[RowConverter[T]](code)
  }

  def annotationImpl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val className = annottees.head.tree match {
      case q"case class $className(..$args) extends ..$parents { ..$body }" =>
        className
      case _ =>
        c.abort(c.enclosingPosition, "The @schema annotation only support public case classes.")
    }

    val tpe = className.toTermName

    val schemaOptionsType = c.weakTypeOf[SchemaOptions]
    val opts = c.prefix.tree match {
      case q"new $atName(..$args)" =>
        q"new $schemaOptionsType(..$args)"
      case _ =>
        q"new $schemaOptionsType"
    }

    val schema = q"""
      implicit val __schema = _root_.com.mediative.sparrow.DataFrameReader.createSchema[$className]($opts)
    """

    val companion = annottees.drop(1).headOption.map { obj =>
      val q"object $objectName extends ..$parents { $self => ..$body }" = obj.tree
      q"""
         object $objectName extends ..$parents { $self =>
          ..$body
          $schema
        }
      """
    } getOrElse {
      q"""
        object $tpe {
          $schema
        }
      """
    }
    c.Expr[Any](q"..${List(annottees.head.tree, companion)}")
  }
}
