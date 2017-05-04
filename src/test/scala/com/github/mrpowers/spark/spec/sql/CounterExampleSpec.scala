package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._

class CounterExampleSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe("#spark-testing-base-issue-1") {

    it("fails to compare dataframes with same content") {

      // Generally speaking, in a dataframe the order of rows has no meaning,
      //   like they are in a sql table
      // But this counter example shows that spark-testing-base doesn't accept that
      // In a spark job, final dataframe is created via multiple transformation
      //    and the rows order is not preserved.
      // So spark-testing-base may not be suitable for testing complicated transformation.

      val firstDf = Seq(
        ("TOM","CAT"),
        ("JERRY","MOUSE")
      ).toDF("first_name","last_name")

      // secondDf has same content, but rows are swapped
      val secondDf = Seq(
        ("JERRY","MOUSE"),
        ("TOM","CAT")
      ).toDF("first_name","last_name")

      var good = 0
      try {
        // spark-testing-base fails here
        assertDataFrameEquals(firstDf, secondDf)
      } catch {
        // we catch the error and claim the test passed
        case _: Throwable => good = 1
      }
      assert(good == 1)

    }

  }

}
