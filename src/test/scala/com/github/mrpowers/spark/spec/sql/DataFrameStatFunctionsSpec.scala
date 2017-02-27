package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._

class DataFrameStatFunctionsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#corr") {

    it("calculates the correlation between two columns") {

      val numbersDf = Seq(
        (1, 8),
        (2, 14),
        (3, 30)
      ).toDF("num1", "num2")

      var res =  numbersDf.stat.corr("num1", "num2")

      assert(res === 0.9672471299049061)

    }

  }

}
