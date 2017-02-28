package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest._

class DataFrameNaFunctionsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#drop") {

    it("drops rows that contains null values") {

      val sourceData = List(
        Row(1, null),
        Row(null, null),
        Row(3, 30),
        Row(10, 20)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.na.drop()

      val expectedData = List(
        Row(3, 30),
        Row(10, 20)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
