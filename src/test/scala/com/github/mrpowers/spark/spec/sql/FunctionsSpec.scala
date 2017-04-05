package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, _}

class FunctionsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#abs") {

    it("calculates the absolute value") {

      val sourceData = List(
        Row(1),
        Row(-8),
        Row(-5),
        Row(null)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.withColumn("num1abs", abs(col("num1")))

      val expectedData = List(
        Row(1, 1),
        Row(-8, 8),
        Row(-5, 5),
        Row(null, null)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num1abs", IntegerType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("acos") {
    pending
  }

  describe("#add_months") {

    it("returns the date that is numMonths after startDate") {

      val sourceDF = Seq(
        ("1", "2016-01-01 00:00:00"),
        ("2", "2016-12-01 00:00:00")
      ).toDF("person_id", "birth_date")
        .withColumn("birth_date", col("birth_date").cast("timestamp"))

      val actualDF = sourceDF.withColumn(
        "future_date",
        add_months(col("birth_date"), 2)
      )

      val expectedDF = Seq(
        ("1", "2016-01-01 00:00:00", "2016-03-01"),
        ("2", "2016-12-01 00:00:00", "2017-02-01")
      ).toDF("person_id", "birth_date", "future_date")
        .withColumn("birth_date", col("birth_date").cast("timestamp"))
        .withColumn("future_date", col("future_date").cast("date"))

      assertDataFrameEquals(actualDF, expectedDF)

    }

  }

  describe("approx_count_distinct") {
    pending
  }

  describe("array_contains") {
    pending
  }

  describe("array") {
    pending
  }

  describe("asc_nulls_first") {
    pending
  }

  describe("asc_nulls_last") {
    pending
  }

  describe("#asc") {

    it("sorts a DataFrame in ascending order") {

      val sourceData = List(
        Row(1),
        Row(-8),
        Row(-5)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.sort(asc("num1"))

      val expectedData = List(
        Row(-8),
        Row(-5),
        Row(1)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("ascii") {
    pending
  }

  describe("asin") {
    pending
  }

  describe("atan") {
    pending
  }

  describe("atan2") {
    pending
  }

  describe("avg") {
    pending
  }

  describe("#ceil") {

    it("rounds the number up to the nearest integer") {

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

  describe("#cos") {

    it("calculates the cosine of the given value") {

      val sourceData = List(
        Row(1),
        Row(2),
        Row(3)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.withColumn("i_am_scared", cos("num1"))

      val expectedData = List(
        Row(1, 0.5403023058681398),
        Row(2, -0.4161468365471424),
        Row(3, -0.9899924966004454)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("i_am_scared", DoubleType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#desc") {

    it("sorts a column in descending order") {

      val sourceData = List(
        Row(1),
        Row(-8),
        Row(-5)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true)
      )

      val sourceDf = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDf = sourceDf.sort(desc("num1"))

      val expectedData = List(
        Row(1),
        Row(-5),
        Row(-8)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)
    }

  }

  describe("#explode") {

    it("returns a new DataFrame where each row has been expanded to zero or more rows by the provided function") {

      val df = Seq(
        ("A", Seq("a", "b", "c"), Seq(1, 2, 3)),
        ("B", Seq("a", "b", "c"), Seq(5, 6, 7))
      ).toDF("id", "class", "num")

      val actualDf = df.select(
        df("id"),
        explode(df("class")).alias("class"),
        df("num")
      )

      val expectedDf = Seq(
        ("A", "a", Seq(1, 2, 3)),
        ("A", "b", Seq(1, 2, 3)),
        ("A", "c", Seq(1, 2, 3)),
        ("B", "a", Seq(5, 6, 7)),
        ("B", "b", Seq(5, 6, 7)),
        ("B", "c", Seq(5, 6, 7))
      ).toDF("id", "class", "num")

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

  describe("#isnull") {

    it("checks column values for null") {

      val wordsDf = Seq(
        (null),
        ("hello"),
        (null),
        (null),
        ("football")
      ).toDF("word")

      val actualDf = wordsDf.withColumn("nullCheck", isnull(col("word")))

      val expectedDf = Seq(
        (null, true),
        ("hello", false),
        (null, true),
        (null, true),
        ("football", false)
      ).toDF("word", "nullCheck")

      assertDataFrameEquals(actualDf, expectedDf)
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

    it("converts the first letter of each word to upper case, returns a new column") {

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

  describe("#sqrt") {

    it("Computes the square root of the specified float value") {

      val numsDF = Seq (
        (49),
        (144),
        (89)
      ).toDF("num1")

      val sqrtDF = numsDF.withColumn("sqrt_num", sqrt(col("num1")))

      val expectedData = List(
        Row(49, 7.0),
        Row(144, 12.0),
        Row(89, 9.4339)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, false),
        StructField("sqrt_num", DoubleType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameApproximateEquals(sqrtDF, expectedDf, 0.01)

    }

  }

  describe("#pow") {

    it("returns the value of the first argument raised to the power of the second argument") {

      val numsDF = Seq (
        (2),
        (3),
        (1)
      ).toDF("num")

      val actualDF = numsDF.withColumn("power", pow(col("num"), 3))

      val expectedData = List(
        Row(2, 8.0),
        Row(3, 27.0),
        Row(1, 1.0)
      )

      val expectedSchema = List(
        StructField("num", IntegerType, false),
        StructField("power", DoubleType, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDf)

    }

  }

  describe("#length") {

    it("returns the length of the column") {

      val expectedSchema = List(
        StructField("word", StringType, true),
        StructField("length", IntegerType, true)
      )

      val expectedData = List(
        Row("banh", 4),
        Row("delilah", 7),
        Row(null, null)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val wordsDf = Seq(
        ("banh"),
        ("delilah"),
        (null)
      ).toDF("word")

      val actualDf = wordsDf.withColumn("length", length(col("word")))

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#countDistinct") {

    it("aggregate function: returns the number of distinct items in a group") {

      val sourceDf = Seq(
        ("A", 1),
        ("B", 1),
        ("A", 2),
        ("A", 2),
        ("B", 3),
        ("A", 3)
      ).toDF("id", "foo")

      val actualDf = sourceDf.groupBy($"id").agg(countDistinct($"foo") as "distinctCountFoo").orderBy($"id")

      val expectedData = List(
        Row("A", 3L),
        Row("B", 2L)
      )

      val expectedSchema = List(
        StructField("id", StringType, true),
        StructField("distinctCountFoo", LongType, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#locate") {

    it("returns index of first occurrence of search string") {

      val wordsDf = Seq(
        ("Spider-man"),
        ("Batman")
      ).toDF("word")

      val actualDf = wordsDf.withColumn("short_word", locate("man", col("word")))

      val expectedData = Seq(
        Row("Spider-man", 8),
        Row("Batman", 4)
      )

      val expectedSchema = List(
        StructField("word", StringType, true),
        StructField("short_word",IntegerType,true)
      )


      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )


      assertDataFrameEquals(actualDf, expectedDf)

    }
  }

}