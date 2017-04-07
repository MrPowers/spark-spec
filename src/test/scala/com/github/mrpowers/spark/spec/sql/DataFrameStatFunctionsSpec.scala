package com.github.mrpowers.spark.spec.sql

import org.scalatest._
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class DataFrameStatFunctionsSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  describe("#approxQuantile") {
    pending
  }

  describe("#bloomFilter") {
    pending
  }

  describe("#corr") {

    it("calculates the correlation between two columns") {

      val numbersDF = Seq(
        (1, 8),
        (2, 14),
        (3, 30)
      ).toDF("num1", "num2")

      var res = numbersDF.stat.corr("num1", "num2")

      assert(res === 0.9672471299049061)

    }

  }

  describe("#countMinSketch") {
    pending
  }

  describe("#cov") {
    pending
  }

  describe("#crosstab") {
    pending
  }

  describe("#freqItems") {
    pending
  }

  describe("#sampleBy") {
    pending
  }

}
