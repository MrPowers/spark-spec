package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, _}

class FunctionsSpec extends FunSpec with ShouldMatchers with DataFrameSuiteBase {

  import spark.implicits._

  describe("#abs") {

    it("calculates the absolute value") {

      val numbersDf = Seq(
        (1),
        (-8),
        (-5)
      ).toDF("num1")

      val actualDf = numbersDf.withColumn("num1abs", abs(col("num1")))

      val expectedDf = Seq(
        (1, 1),
        (-8, 8),
        (-5, 5)
      ).toDF("num1", "num1abs")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#ceil") {

    it("calculates the absolute value") {

      val numbersDf = Seq(
        (1.5),
        (-8.1),
        (5.9)
      ).toDF("num1")

      val actualDf = numbersDf.withColumn("upper", ceil(col("num1")))

      val expectedData = List(
        Row(1.5, 2L),
        Row(-8.1, -8L),
        Row(5.9, 6L)
      )

      val expectedSchema = List(
        StructField("num1", DoubleType, false),
        StructField("upper", LongType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#coalesce") {

    it("returns the first column that is not null, or null if all inputs are null.") {

      val wordsDf = Seq(
        ("banh", "mi"),
        ("pho", "ga"),
        (null, "cheese"),
        ("pizza", null),
        (null, null)
      ).toDF("word1", "word2")

      val actualDf = wordsDf.withColumn(
        "yummy",
        coalesce(
          col("word1"),
          col("word2")
        )
      )

      val expectedDf = Seq(
        ("banh", "mi", "banh"),
        ("pho", "ga", "pho"),
        (null, "cheese", "cheese"),
        ("pizza", null, "pizza"),
        (null, null, null)
      ).toDF("word1", "word2", "yummy")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#concat") {

    it("concatenates multiple input string columns together into a single string column") {

      val wordsDf = Seq(
        ("banh", "mi"),
        ("pho", "ga"),
        (null, "cheese"),
        ("pizza", null),
        (null, null)
      ).toDF("word1", "word2")

      val actualDf = wordsDf.withColumn(
        "yummy",
        concat(
          col("word1"),
          col("word2")
        )
      )

      val expectedDf = Seq(
        ("banh", "mi", "banhmi"),
        ("pho", "ga", "phoga"),
        (null, "cheese", null),
        ("pizza", null, null),
        (null, null, null)
      ).toDF("word1", "word2", "yummy")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#concat_ws") {

    it("concatenates multiple input string columns with separator") {

      val wordsDf = Seq(
        ("banh", "mi"),
        ("pho", "ga"),
        (null, "cheese"),
        ("pizza", null),
        (null, null)
      ).toDF("word1", "word2")

      val actualDf = wordsDf.withColumn(
        "yummy",
        concat_ws(
          "_",
          col("word1"),
          col("word2")
        )
      )

      val expectedData = List(
        Row("banh", "mi", "banh_mi"),
        Row("pho", "ga", "pho_ga"),
        Row(null, "cheese", "cheese"), // null column will be omitted
        Row("pizza", null, "pizza"), // null column will be omitted
        Row(null, null, "") // all null columns give ""
      )

      val expectedSchema = List(
        StructField("word1", StringType, true),
        StructField("word2", StringType, true),
        StructField("yummy", StringType, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#factorial"){

    it("calculates the product of an integer and all the integers below"){

      val inputSchema = List(StructField("number",IntegerType,false))

      val inputData = List(
        Row(0),Row(1),Row(2),Row(3),Row(4),Row(5),Row(6)
      )

      val inputDf = spark.createDataFrame(
        spark.sparkContext.parallelize(inputData),
        StructType(inputSchema)
      )

      val expectedSchema = List(
        StructField("number",IntegerType,false),
        StructField("result",LongType,true)
      )

      val expectedData = List(
        Row(0,1L),
        Row(1,1L),
        Row(2,2L),
        Row(3,6L),
        Row(4,24L),
        Row(5,120L),
        Row(6,720L)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val actualDf = inputDf.withColumn("result", factorial(col("number")))

      assertDataFrameEquals(actualDf,expectedDf)

    }

  }
   
  describe("#lower") {

    it("converts a string to lower case") {

      val wordsDf = Seq(
        ("Batman"),
        ("CATWOMAN"),
        ("pikachu")
      ).toDF("word")

      val actualDf = wordsDf.withColumn("lower_word", lower(col("word")))

      val expectedDf = Seq(
        ("Batman", "batman"),
        ("CATWOMAN", "catwoman"),
        ("pikachu", "pikachu")
      ).toDF("word", "lower_word")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#rpad") {

    it("Right-padded with pad to a length of len") {

      val wordsDf = Seq(
        ("banh"),
        ("delilah"),
        (null),
        ("c")
      ).toDF("word1")


      val actualDf = wordsDf.withColumn("rpad_column", rpad(col("word1"), 5, "x"))

      val expectedDf = Seq(
        ("banh", "banhx"),
        ("delilah", "delil"),
        (null, null),
        ("c", "cxxxx")
      ).toDF("word1", "rpad_column")

      assertDataFrameEquals(actualDf, expectedDf)


    }

  }

  describe("#trim") {

    it("converts a string to lower case") {

      val wordsDf = Seq(
        ("bat  "),
        ("  cat")
      ).toDF("word")

      val actualDf = wordsDf.withColumn("short_word", trim(col("word")))

      val expectedDf = Seq(
        ("bat  ", "bat"),
        ("  cat", "cat")
      ).toDF("word", "short_word")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#upper") {

    it("converts a string to upper case") {

      val wordsDf = Seq(
        ("BatmaN"),
        ("boO"),
        ("piKachu")
      ).toDF("word")

      val actualDf = wordsDf.withColumn("upper_word", upper(col("word")))

      val expectedDf = Seq(
        ("BatmaN", "BATMAN"),
        ("boO", "BOO"),
        ("piKachu", "PIKACHU")
      ).toDF("word", "upper_word")

      assertDataFrameEquals(actualDf, expectedDf)

    }
  }

  describe("#initcap") {

    it("converts a the first letter of each word to upper case, returns a new column") {

      val wordsDf = Seq(
        ("bat man"),
        ("cat woman"),
        ("spider man")
      ).toDF("no_upper_words")

      val actualDf = wordsDf.withColumn("first_upper", initcap(col("no_upper_words")))

      val expectedDf = Seq(
        ("bat man", "Bat Man"),
        ("cat woman", "Cat Woman"),
        ("spider man", "Spider Man")
      ).toDF("no_upper_words", "first_upper")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }
}