package com.github.mrpowers.spark.spec.sql.types

import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField}
import org.scalatest.FunSpec

class StructFieldSpec extends FunSpec {

  describe("#new") {

    it("can be instantiated with shorthand notation") {

      val s = StructField("num1", IntegerType, true)
      assert(s.getClass().getName() === "org.apache.spark.sql.types.StructField")

    }

    it("can be instantiated with explicit notation") {

      val s = new StructField("num1", IntegerType, true)
      assert(s.getClass().getName() === "org.apache.spark.sql.types.StructField")

    }

  }

  describe("#canEqual") {
    pending
  }

  describe("#dataType") {

    it("returns the dataType") {

      val s = StructField("num1", IntegerType, true)
      assert(s.dataType === IntegerType)

    }

  }

  describe("#equals") {

    it("returns true if two StructFields are equal") {

      val s1 = StructField("num1", IntegerType, true)
      val s2 = StructField("num1", IntegerType, true)
      assert(s1.equals(s2) === true)

    }

  }

  describe("#getComment") {

    it("returns the comment from the Metadata") {

      val s = StructField(
        "cl1",
        IntegerType,
        nullable = false,
        new MetadataBuilder().putString("comment", "crazy").build()
      )
      assert(s.getComment() === Some("crazy"))

    }

  }

  describe("#metadata") {

    it("returns the metadata object") {

      val m = new MetadataBuilder().putString("comment", "crazy").build()

      val s = StructField(
        "cl1",
        IntegerType,
        nullable = false,
        m
      )
      assert(s.metadata === m)

    }

  }

  describe("#name") {

    it("returns the name of a StructField") {

      val s = StructField("num1", IntegerType, true)
      assert(s.name === "num1")

    }

  }

  describe("#nullable") {

    it("returns true if the StructField is nullable") {

      val s = StructField("num1", IntegerType, true)
      assert(s.nullable === true)

    }

    it("returns false if the StructField is not nullable") {

      val s = StructField("num1", IntegerType, false)
      assert(s.nullable === false)

    }

  }

  describe("#productArity") {
    pending
  }

  describe("#productElement") {
    pending
  }

  describe("#productIterator") {
    pending
  }

  describe("#productPrefix") {

    it("???") {

      val s = StructField("num1", IntegerType, true)
      assert(s.productPrefix === "StructField")

    }

  }

  describe("#toString") {

    it("converts a StructField to a string") {

      val s = StructField("num1", IntegerType, true)
      assert(s.toString() === "StructField(num1,IntegerType,true)")

    }

  }

  describe("#withComment") {

    it("adds a comment to the metadata") {

      val s = StructField("num1", IntegerType, true)
      val c = s.withComment("boo")
      assert(c.getComment() === Some("boo"))

    }

  }

}
