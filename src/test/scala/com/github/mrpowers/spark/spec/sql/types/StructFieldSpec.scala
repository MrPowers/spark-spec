package com.github.mrpowers.spark.spec.sql.types

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{IntegerType, StructField}
import org.scalatest.FunSpec

class StructFieldSpec extends FunSpec with DataFrameSuiteBase {

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
    pending
  }

  describe("#equals") {
    pending
  }

  describe("#getComment") {
    pending
  }

  describe("#metadata") {
    pending
  }

  describe("#name") {
    pending
  }

  describe("#nullable") {
    pending
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
    pending
  }

  describe("#toString") {
    pending
  }

  describe("#withComment") {
    pending
  }

}
