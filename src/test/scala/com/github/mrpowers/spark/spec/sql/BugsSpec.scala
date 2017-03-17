package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.scalatest.FunSpec

class BugsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("Dataset#withColumnRenamed") {

    it("incorrectly renames a column in a case insensitive manner") {

      val sourceDF = Seq(1).toDF("foo")

      val actualDF = sourceDF.withColumnRenamed("FOO", "bar")

      val expectedDF = Seq(1).toDF("bar")

      assertDataFrameEquals(actualDF, expectedDF)

    }

  }

}
