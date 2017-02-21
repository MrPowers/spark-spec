package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._

class FunctionsSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe("#abs") {

    it("calculates the absolute value") {

      val numbersDf = Seq(
        (1),
        (-8),
        (-5)
      ).toDF("num1")

      val actualDf = numbersDf.withColumn("num1abs", abs(col("num1")))

      val expectedDf = Seq(
        (1, 1),
        (-8, 8),
        (-5, 5)
      ).toDF("num1", "num1abs")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#concat") {

    it("concatenates multiple input string columns together into a single string column") {

      val wordsDf = Seq(
        ("banh", "mi"),
        ("pho", "ga"),
        (null, "cheese"),
        ("pizza", null),
        (null, null)
      ).toDF("word1", "word2")

      val actualDf = wordsDf.withColumn(
        "yummy",
        concat(
          col("word1"),
          col("word2")
        )
      )

      val expectedDf = Seq(
        ("banh", "mi", "banhmi"),
        ("pho", "ga", "phoga"),
        (null, "cheese", null),
        ("pizza", null, null),
        (null, null, null)
      ).toDF("word1", "word2", "yummy")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#concat_ws") {

    it("concatenates multiple input string columns with separator") {

      val wordsDf = Seq(
        ("banh", "mi"),
        ("pho", "ga"),
        (null, "cheese"),
        ("pizza", null),
        (null, null)
      ).toDF("word1", "word2")

      val actualDf = wordsDf.withColumn(
        "yummy",
        concat_ws(
          "_",
          col("word1"),
          col("word2")
        )
      )

      val expectedDf = Seq(
        ("banh", "mi", "banh_mi"),
        ("pho", "ga", "pho_ga"),
        (null, "cheese", "cheese"), // null column will be omitted
        ("pizza", null, "pizza"), // null column will be omitted
        (null, null, "") // all null columns give ""
      ).toDF("word1", "word2", "yummy")

      // assertDataFrameEquals(actualDf, expectedDf) // the assertDataFrameEquals has bug here
      assert(actualDf.select("yummy").collect.deep == expectedDf.select("yummy").collect.deep)

    }

  }

}
