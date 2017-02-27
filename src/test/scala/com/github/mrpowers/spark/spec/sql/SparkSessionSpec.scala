package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest._

class SparkSessionSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  describe("#read") {

    it("reads a CSV file into a DataFrame") {

      val path = new java.io.File("./src/test/resources/people.csv").getCanonicalPath

      val actualDf = spark.read.option("header", "true").csv(path)

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

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("version") {

    it("returns the version of Spark on which this application is running") {

      assert(spark.version === "2.1.0")

    }

  }

}
