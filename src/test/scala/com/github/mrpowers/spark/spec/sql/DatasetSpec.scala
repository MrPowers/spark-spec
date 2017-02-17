package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class DatasetSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe("#agg") {

    it("HACK - don't know what this does") {

      val sourceDf = Seq(
        ("jose", "blue"),
        ("li", "blue"),
        ("luisa", "red")
      ).toDF("name", "color")

      val df = sourceDf.agg(max(col("color")))

      // HACK - this isn't getting me what I want
      // might need to ask Milin for help

    }

  }

  describe("#alias") {

    it("aliases a DataFrame") {

      val sourceDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDf = sourceDf.select(col("name").alias("student"))

      val expectedDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("student")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  // MISSING - #apply

  describe("#as") {

    it("does the same thing as alias") {

      val sourceDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDf = sourceDf.select(col("name").as("student"))

      val expectedDf = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("student")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  // MISSING - cache
  // MISSING - checkpoint
  // MISSING - classTag
  // MISSING - coalesce
  // MISSING - col

  describe("#collect") {

    it("returns an array of Rows in the DataFrame") {

      val row1 = Row("cat")
      val row2 = Row("dog")

      val sourceData = List(
        row1,
        row2
      )

      val sourceSchema = List(
        StructField("animal", StringType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val s = sourceDf.collect()

      s should equal(Array(row1, row2))

    }

  }

  // MISSING - collectAsList

  describe("#columns") {

    it("returns all the column names as an array") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val expected = Array("team", "sport")

      sourceDf.columns should equal(expected)

    }

  }

  describe("#count") {

    it("returns a count of all the rows in a DataFrame") {

      val sourceDf = Seq(
        ("jets"),
        ("barcelona")
      ).toDF("team")

      sourceDf.count should equal(2)

    }

  }

  // MISSING - createGlobalTempView
  // MISSING - createOrReplaceTempView
  // MISSING - createTempView

  describe("#crossJoin") {

    it("cross joins two DataFrames") {

      val letterDf = Seq(
        ("a"),
        ("b")
      ).toDF("letter")

      val numberDf = Seq(
        ("1"),
        ("2")
      ).toDF("number")

      val actualDf = letterDf.crossJoin(numberDf)

      val expectedDf = Seq(
        ("a", "1"),
        ("a", "2"),
        ("b", "1"),
        ("b", "2")
      ).toDF("letter", "number")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  // MISSING - cube

  describe("#describe") {

    it("provides analytic statistics for a numeric column") {

      val numbersDf = Seq(
        (1),
        (8),
        (5)
      ).toDF("num1")

      val actualDf = numbersDf.describe()

      val expectedDf = Seq(
        ("count", "3"),
        ("mean", "4.666666666666667"),
        ("stddev", "3.5118845842842465"),
        ("min", "1"),
        ("max", "8")
      ).toDF("summary", "num1")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("only provides certain descriptive stats for a string column") {

      val letterDf = Seq(
        ("a"),
        ("b")
      ).toDF("letter")

      val actualDf = letterDf.describe()

      val expectedDf = Seq(
        ("count", "2"),
        ("mean", null),
        ("stddev", null),
        ("min", "a"),
        ("max", "b")
      ).toDF("summary", "letter")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#distinct") {

    it("returns the unique rows in a DataFrame") {

      val numbersDf = Seq(
        (1, 2),
        (8, 8),
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      val actualDf = numbersDf.distinct()

      val expectedDf = Seq(
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("drop") {

    it("drops a column from a DataFrame") {

      val peopleDf = Seq(
        ("larry", true),
        ("jeff", false),
        ("susy", false)
      ).toDF("person", "wearGlasses")

      val actualDf = peopleDf.drop("wearGlasses")

      val expectedDf = Seq(
        ("larry"),
        ("jeff"),
        ("susy")
      ).toDF("person")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#dropDuplicates") {

    it("drops the duplicate rows from a DataFrame") {

      val numbersDf = Seq(
        (1, 2),
        (8, 8),
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      val actualDf = numbersDf.dropDuplicates()

      val expectedDf = Seq(
        (1, 2),
        (5, 6),
        (8, 8)
      ).toDF("num1", "num2")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("drops duplicate rows based on certain columns") {

      val numbersDf = Seq(
        (1, 2, 100),
        (8, 8, 100),
        (1, 2, 200),
        (5, 6, 7),
        (8, 8, 50)
      ).toDF("num1", "num2", "num3")

      val actualDf = numbersDf.dropDuplicates("num1", "num2")

      val sourceData = List(
        Row(1, 2, 100),
        Row(5, 6, 7),
        Row(8, 8, 100)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, false),
        StructField("num2", IntegerType, false),
        StructField("num3", IntegerType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#dtypes") {

    it("returns the column names and their data types as an array") {

      val abcDf = Seq(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      ).toDF("letter", "number")

      val actual = abcDf.dtypes
      val expected = Array(("letter", StringType), ("number", IntegerType))

      // HACK - couldn't get this to work
      // Don't know how to do Array equality with Scala

      // actual.deep should equal(expected.deep)

    }

  }

  describe("#except") {

    it("returns a new Dataset with the rows in this Dataset but not in another Dataset") {

      val numbersDf = Seq(
        (1, 2),
        (4, 5),
        (8, 9)
      ).toDF("num1", "num2")

      val moreDf = Seq(
        (100, 200),
        (4, 5),
        (800, 900)
      ).toDF("num1", "num2")

      val actualDf = numbersDf.except(moreDf)

      val expectedDf = Seq(
        (8, 9),
        (1, 2)
      ).toDF("num1", "num2")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  // MISSING - explain

  describe("#filter") {

    it("filters rows based on a given condition") {

      val numbersDf = Seq(
        (1),
        (4),
        (8),
        (42)
      ).toDF("num1")

      val actualDf = numbersDf.filter(col("num1") > 5)

      val expectedDf = Seq(
        (8),
        (42)
      ).toDF("num1")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("filters rows based on a SQL condition") {

      val numbersDf = Seq(
        (1),
        (4),
        (8),
        (42)
      ).toDF("num1")

      val actualDf = numbersDf.filter("num1 != 8")

      val expectedDf = Seq(
        (1),
        (4),
        (42)
      ).toDF("num1")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#first") {

    it("returns the first row of a DataFrame") {

      val row1 = Row("doug")
      val row2 = Row("patty")

      val sourceData = List(
        row1,
        row2
      )

      val sourceSchema = List(
        StructField("character", StringType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      sourceDf.first() should equal(row1)

    }

  }

  describe("#flatMap") {

    it("replaces explode and provides flexibility") {

//       HACK - can't figure out how this works :(

//      val wordsDf = Seq(
//        ("the people like to do the stuff"),
//        ("farmers like the rain")
//      ).toDF("sentence")
//
//      wordsDf.flatMap(_.sentence.split(" "))
//      wordsDf.flatMap(_.size)
//
//      case class Sentence(words: String)
//
//      val ds = Seq(
//        Sentence("the people like to do the stuff"),
//        Sentence("farmers like the rain")
//      ).toDS()
//
//      ds.flatMap(_.sentence.split(" ")).show()

    }

  }

  // MISSING - foreach
  // MISSING - foreachPartition

  describe("#groupBy") {

    it("groups columns for aggregations") {

      val playersDf = Seq(
        (1, "boston"),
        (4, "boston"),
        (8, "detroit"),
        (42, "detroit")
      ).toDF("score", "team")

      val actualDf = playersDf.groupBy("team").sum("score")

      val expectedData = List(
        Row("boston", 5.toLong),
        Row("detroit", 50.toLong)
      )

      val expectedSchema = List(
        StructField("team", StringType, true),
        StructField("sum(score)", LongType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  // MISSING - groupByKey

  describe("#head") {

    it("returns the first row") {

      val row1 = Row("doug")
      val row2 = Row("patty")

      val sourceData = List(
        row1,
        row2
      )

      val sourceSchema = List(
        StructField("character", StringType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      sourceDf.head() should equal(row1)

    }

    it("returns the first n rows") {

      val row1 = Row("doug")
      val row2 = Row("patty")
      val row3 = Row("frank")

      val sourceData = List(
        row1,
        row2
      )

      val sourceSchema = List(
        StructField("character", StringType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      sourceDf.head(2) should equal(Array(row1, row2))

    }

  }

  // MISSING - inputFiles

  describe("#intersect") {

    it("returns a DataFrames that contains the rows in both the DataFrames") {

      val numbersDf = Seq(
        (1, 2),
        (4, 5),
        (8, 9)
      ).toDF("num1", "num2")

      val moreDf = Seq(
        (100, 200),
        (4, 5),
        (800, 900),
        (1, 2)
      ).toDF("num1", "num2")

      val actualDf = numbersDf.intersect(moreDf)

      val expectedDf = Seq(
        (1, 2),
        (4, 5)
      ).toDF("num1", "num2")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  // MISSING - isLocal
  // MISSING - isStreaming
  // MISSING - javaRDD

  describe("#join") {

    it("joins two DataFrames") {

      val peopleDf = Seq(
        ("larry", "1"),
        ("jeff", "2"),
        ("susy", "3")
      ).toDF("person", "id")

      val birthplaceDf = Seq(
        ("new york", "1"),
        ("ohio", "2"),
        ("los angeles", "3")
      ).toDF("city", "person_id")

      val actualDf = peopleDf.join(
        birthplaceDf, peopleDf("id") <=> birthplaceDf("person_id")
      )

      val expectedDf = Seq(
        ("larry", "1", "new york", "1"),
        ("jeff", "2", "ohio", "2"),
        ("susy", "3", "los angeles", "3")
      ).toDF("person", "id", "city", "person_id")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }


}
