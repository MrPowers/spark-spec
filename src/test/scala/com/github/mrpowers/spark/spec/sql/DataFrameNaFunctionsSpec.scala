package com.github.mrpowers.spark.spec.sql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DatasetComparer

class DataFrameNaFunctionsSpec extends FunSpec with SparkSessionTestWrapper with DatasetComparer {

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

      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDF = sourceDF.na.drop()

      val expectedData = List(
        Row(3, 30),
        Row(10, 20)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#fill") {

    it("Returns a new DataFrame that replaces null or NaN values in numeric columns with value") {

      val sourceDF = spark.createDF(
        List(
          (1, null),
          (null, null),
          (3, 30),
          (10, 20)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true)
        )
      )

      val actualDF = sourceDF.na.fill(77)

      val expectedDF = spark.createDF(
        List(
          (1, 77),
          (77, 77),
          (3, 30),
          (10, 20)
        ), List(
          ("num1", IntegerType, false),
          ("num2", IntegerType, false)
        )
      )

      assertSmallDatasetEquality(actualDF, expectedDF)

    }

  }

  describe("#replace") {
    pending
  }

}
