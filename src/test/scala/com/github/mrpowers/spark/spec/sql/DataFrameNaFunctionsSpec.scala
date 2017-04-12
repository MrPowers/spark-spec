package com.github.mrpowers.spark.spec.sql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
import org.scalatest._

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class DataFrameNaFunctionsSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

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

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#fill") {

    it("Returns a new DataFrame that replaces null or NaN values in numeric columns with value") {

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

      val actualDF = sourceDF.na.fill(77)

      val expectedData = List(
        Row(1, 77),
        Row(77, 77),
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

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#replace") {
    pending
  }

}
