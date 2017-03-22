package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.scalatest._

class DataFrameSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#rollup") {

    it("Create a multi-dimensional rollup for the current DataFrame using the specified columns, so we can run aggregation on them") {

      val sourceData = Seq(
        ("1", "A", 1000),
        ("2", "A", 2000),
        ("1", "B", 2000),
        ("2", "B", 4000)
      ).toDF("department", "group", "money")

      val actualDF = sourceData.rollup(col("group")).sum().withColumnRenamed("sum(money)", "money").orderBy(col("group"))

      val expectedData = List(
        Row(null, 9000L),
        Row("A", 3000L),
        Row("B", 6000L)
      )

      val expectedSchema = List(
        StructField("group", StringType, true),
        StructField("money", LongType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDF)

    }
  }
}