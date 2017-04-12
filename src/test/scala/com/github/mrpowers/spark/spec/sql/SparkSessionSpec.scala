package com.github.mrpowers.spark.spec.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest._
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class SparkSessionSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  describe("#baseRelationToDataFrame") {
    pending
  }

  describe("#builder") {
    pending
  }

  describe("#catalog") {
    pending
  }

  describe("#clearActiveSession") {
    pending
  }

  describe("#clearDefaultSession") {
    pending
  }

  describe("#close") {
    pending
  }

  describe("#conf") {
    pending
  }

  describe("#createDataFrame") {
    pending
  }

  describe("#createDataset") {
    pending
  }

  describe("#emptyDataFrame") {
    pending
  }

  describe("#emptyDataset") {
    pending
  }

  describe("#experimental") {
    pending
  }

  describe("#implicits") {
    pending
  }

  describe("#listenerManager") {
    pending
  }

  describe("#newSession") {
    pending
  }

  describe("#range") {
    pending
  }

  describe("#read") {

    it("reads a CSV file into a DataFrame") {

      val path = new java.io.File("./src/test/resources/people.csv").getCanonicalPath

      val actualDF = spark.read.option("header", "true").csv(path)

      val expectedSchema = List(
        StructField("name", StringType, true),
        StructField("country", StringType, true),
        StructField("zip_code", StringType, true)
      )

      val expectedData = List(
        Row("joe", "usa", "89013"),
        Row("ravi", "india", null),
        Row(null, null, "12389")
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#readStream") {
    pending
  }

  describe("#setActiveSession") {
    pending
  }

  describe("#sql") {
    pending
  }

  describe("#sqlContext") {
    pending
  }

  describe("#stop") {
    pending
  }

  describe("#streams") {
    pending
  }

  describe("#table") {
    pending
  }

  describe("#time") {
    pending
  }

  describe("#udf") {
    pending
  }

  describe("version") {

    it("returns the version of Spark on which this application is running") {

      assert(spark.version === "2.1.0")

    }

  }

}
