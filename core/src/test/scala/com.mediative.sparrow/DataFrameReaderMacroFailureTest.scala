package com.mediative.sparrow

import org.scalatest._
import Matchers._

class DataFrameReaderMacroFailureTest extends FreeSpec {

  "createSchema" - {

    "works for a proper case class" in {
      """
      import com.mediative.sparrow._
      object Wrapper {
        class Simple(first: String, second: Int)
        object Simple {
          val options = SchemaOptions()
          implicit val schema = DataFrameReader.createSchema[Simple](options)
          def apply(f: String, s: Int) = new Simple(f, s)
        }
      }
      """ should compile
    }

    def checkError(error: exceptions.TestFailedException)(expected: String) = {
      assert(error.getMessage.startsWith(
        s"""Expected no compiler error, but got the following type error: "$expected", for code:"""))
    }

    "fail for less two two fields for a case class" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          case class Simple(first: String)
          object Simple {
            val options = SchemaOptions()
            DataFrameReader.createSchema[Simple](options)
          }
        }
        """ should compile
      }

      checkError(error) {
        "Only case classes with more than one field are supported."
      }
    }

    "fail if T doesn't have an apply method" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          class Simple(first: String, second: Int)
          object Simple {
            val options = SchemaOptions()
            DataFrameReader.createSchema[Simple](options)
          }
        }
        """ should compile
      }

      checkError(error) {
        "Cannot find an apply method with the proper signature."
      }
    }

    "fail if T doesn't have an apply method with the proper return type" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          class Simple(first: String, second: Int)
          object Simple {
            val options = SchemaOptions()
            DataFrameReader.createSchema[Simple](options)
            def apply(first: String, second: Int) = first + second
          }
        }
        """ should compile
      }

      checkError(error) {
        "Cannot find an apply method with the proper signature."
      }
    }

    "fail if T doesn't have an apply method with the proper argument count" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          class Simple(first: String, second: Int)
          object Simple {
            val options = SchemaOptions()
            DataFrameReader.createSchema[Simple](options)
            def apply(first: String) = new Simple(first, 3)
          }
        }
        """ should compile
      }

      checkError(error) {
        "Cannot find an apply method with the proper signature."
      }
    }

    "fail if T doesn't have an apply method with the proper argument types" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          class Simple(first: String, second: Int)
          object Simple {
            val options = SchemaOptions()
            DataFrameReader.createSchema[Simple](options)
            def apply(second: Int, first: String) = new Simple(first, second)
          }
        }
        """ should compile
      }

      checkError(error) {
        "Cannot find an apply method with the proper signature."
      }
    }

    "fail if @embedded and @fieldOptions is used on the same field" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          case class Outer(first: String, @embedded("prefix") @fieldName("Inner") inner: Inner)
          case class Inner(first: String, second: Int)
          object Outer {
            val options = SchemaOptions()
            DataFrameReader.createSchema[Outer](options)
          }
        }
        """ should compile
      }

      checkError(error) {
        "@embedded and @fieldName or @fieldOption cannot be used on the same field."
      }
    }

    "fail if @schema is used on a class that isn't a case class" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          @schema
          class Simple(first: String, second: Int)
        }
        """ should compile
      }

      checkError(error) {
        "The @schema annotation only support public case classes."
      }
    }

    "fail if @schema is used on something else than a case class" in {
      val error = intercept[exceptions.TestFailedException] {
        """
        import com.mediative.sparrow._
        object Wrapper {
          @schema
          val first: String = "first"
        }
        """ should compile
      }

      checkError(error) {
        "The @schema annotation only support public case classes."
      }
    }
  }
}
