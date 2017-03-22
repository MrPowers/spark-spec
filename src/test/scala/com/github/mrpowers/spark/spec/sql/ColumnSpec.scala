package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, _}

class ColumnSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#alias") {

    it("gives the column an alias") {

      val sourceDF = Seq(
        ("gary", 42),
        ("bristol", 12)
      ).toDF("name", "age")

      val actualDF = sourceDF.select(
        col("name").alias("city"),
        col("age").alias("num_people")
      )

      val expectedDF = Seq(
        ("gary", 42),
        ("bristol", 12)
      ).toDF("city", "num_people")

      assertDataFrameEquals(actualDF, expectedDF)

    }

  }

  describe("#and") {

    it ("returns true if both columns are true") {

      val sourceDf = Seq(
        ("ted", true, false),
        ("mary", true, true),
        ("brian", false, true)
      ).toDF("name", "like_cheese", "is_tall")

      val actualDf = sourceDf.where(col("like_cheese").and(col("is_tall")))

      val expectedDf = Seq(
        ("mary", true, true)
      ).toDF("name", "like_cheese", "is_tall")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#apply") {
    pending
  }

  describe("#as") {

    it("gives the column an alias with metadata") {

      val sourceDF = Seq(
        ("gary", 42),
        ("bristol", 12)
      ).toDF("name", "age")

      val actualDF = sourceDF.select(
        col("name").as("city"),
        col("age").as("num_people")
      )

      val expectedDF = Seq(
        ("gary", 42),
        ("bristol", 12)
      ).toDF("city", "num_people")

      assertDataFrameEquals(actualDF, expectedDF)

    }

  }

  describe("#asc_nulls_first") {

    it("sorts a column in ascending order with the null values first") {

      val sourceData = Seq(
        Row(null, null),
        Row("gary", 42),
        Row("bristol", 12),
        Row("frank", 60),
        Row("abdul", 14),
        Row(null, 99)
      )

      val sourceSchema = List(
        StructField("first_name", StringType, true),
        StructField("age", IntegerType, true)
      )

      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val actualDF = sourceDF.sort(
        asc_nulls_first("first_name")
      )

      val expectedData = Seq(
        Row(null, null),
        Row(null, 99),
        Row("abdul", 14),
        Row("bristol", 12),
        Row("frank", 60),
        Row("gary", 42)
      )

      val expectedSchema = List(
        StructField("first_name", StringType, true),
        StructField("age", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDF)

    }
  }

  describe("#asc") {
    pending
  }

  describe("#between") {
    pending
  }

  describe("#bitwiseAND") {
    pending
  }

  describe("#bitwiseOR") {
    pending
  }

  describe("#cast") {
    pending
  }

  describe("#contains") {

    it("To check if an element is present in a sequence ") {

      val sourceDf = Seq(
        ("Scala is scalable and cool"),
        ("I like peanuts"),
        (null)
      ).toDF("text")

      val actualDf = sourceDf.filter($"text".contains("Scala"))

      val expectedDf = Seq(
        ("Scala is scalable and cool")
      ).toDF("text")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#desc_nulls_first") {
    pending
  }

  describe("#desc_nulls_last") {
    pending
  }

  describe("#desc") {
    pending
  }

  describe("#divide") {
    pending
  }

  describe("#endsWith") {

    it("string ends with") {

      val sourceDf = Seq(
        ("A","1970"),
        ("B","1986"),
        ("C","1990"),
        ("D","2000"),
        ("E","2012"))
        .toDF("id", "year")


      val actualDf = sourceDf.where(col("year").endsWith("0"))

      val expectedDf = Seq(
        ("A","1970"),
        ("C","1990"),
        ("D","2000"))
        .toDF("id", "year")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#eqNullSafe") {
    pending
  }

  describe("#equals") {
    pending
  }

  describe("#equalTo") {
    pending
  }

  describe("#explain") {
    pending
  }

  describe("#expr") {
    pending
  }

  describe("#geq") {

    it("matches for greater than or equal to an expression") {

      val sourceDf = Seq(
        ("1980-09-10"),
        ("1990-04-18"),
        ("2000-04-18"),
        ("2010-04-18"),
        ("2016-01-10")
      ).toDF("some_date")
        .withColumn("some_date", $"some_date".cast("timestamp"))

      val actualDf = sourceDf.where(col("some_date").geq("1995-04-18"))

      val expectedDf = Seq(
        ("2000-04-18"),
        ("2010-04-18"),
        ("2016-01-10")
      ).toDF("some_date")
        .withColumn("some_date", $"some_date".cast("timestamp"))

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#getField") {
    pending
  }

  describe("#getItem") {

    it("An expression that gets an item at position ordinal out of an array, or gets a value by key key in a MapType") {

      val sourceDf = Seq(
        ("A","1970-10-02"),
        ("B","1986-12-30"),
        ("C","1990-12-03"),
        ("D","2000-04-22"),
        ("E","2012-09-09"))
        .toDF("id", "birthday")


      val actualDf = sourceDf.withColumn("month", split(col("birthday"), "-").getItem(1))

      val expectedDf = Seq(
        ("A","1970-10-02", "10"),
        ("B","1986-12-30", "12"),
        ("C","1990-12-03", "12"),
        ("D","2000-04-22", "04"),
        ("E","2012-09-09", "09"))
        .toDF("id", "birthday", "month")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#gt") {

    it("keeps rows greater than a certain number") {

      val sourceDf = Seq(
        (45),
        (79),
        (124),
        (196),
        (257)
      ).toDF("num1")

      val actualDf = sourceDf.where(col("num1").gt(124))

      val expectedDf = Seq(
        (196),
        (257)
      ).toDF("num1")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#hashCode") {
    pending
  }

  describe("#isin") {

    it("A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments") {

      val sourceDf = Seq(
        ("nate"),
        ("tim"),
        ("Tim"),
        ("miller"),
        ("jackson"),
        ("andrew")
      ).toDF("some_name")

      val actualDf = sourceDf.where(col("some_name").isin("tim","andrew","noname"))

      val expectedDf = Seq(
        ("tim"),
        ("andrew")
      ).toDF("some_name")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#isNaN") {
    pending
  }

  describe("#isNotNull") {
    pending
  }

  describe("#isNull") {
    pending
  }

  describe("#leq") {
    pending
  }

  describe("#like") {
    pending
  }

  describe("#lt") {
    pending
  }

  describe("#minus") {

    it("Subtraction. Subtract the other expression from this expression.") {

      val sourceDf = Seq(
        (45),
        (79),
        (55),
        (124)
      ).toDF("num1")

      val actualDf = sourceDf.select(
        sourceDf.col("num1"),
        sourceDf.col("num1").minus(55).as("num2")
      )

      val expectedData = List(
        Row(45, -10),
        Row(79, 24),
        Row(55, 0),
        Row(124, 69)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, false),
        StructField("num2", IntegerType, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#mod") {
    it("return modulo, remainder") {

      val sourceDf = Seq(
        ("A", "1990"),
        ("B", "1998"),
        ("C", "2011"),
        ("D", "2014"),
        ("E", "2017"),
        ("F", "2016"),
        ("G", "1993"))
        .toDF("id", "year")

      val actualDf = sourceDf.filter(col("year").mod(2) === 0)

      val expectedDf = Seq(
        ("A", "1990"),
        ("B", "1998"),
        ("D", "2014"),
        ("F", "2016"))
        .toDF("id", "year")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#multiply") {

    it("Multiplication of this expression and another expression.") {

      val sourceDf = Seq(
        (7),
        (14),
        (23),
        (41)
      ).toDF("num1")

      val actualDf = sourceDf.select(
        sourceDf.col("num1"),
        sourceDf.col("num1").multiply(5).as("num2")
      )

      val expectedData = List(
        Row(7, 35),
        Row(14, 70),
        Row(23, 115),
        Row(41, 205)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, false),
        StructField("num2", IntegerType, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#name") {
    pending
  }

  describe("#notEqual") {
    pending
  }

  describe("#or") {
    pending
  }

  describe("#otherwise") {
    pending
  }

  describe("#over") {
    pending
  }

  describe("#plus") {

    it("sum of this expression and another expression") {

      val sourceDf = Seq(
        (45),
        (79),
        (0),
        (124)
      ).toDF("num1")

      val actualDf = sourceDf.select(
        sourceDf.col("num1"),
        sourceDf.col("num1").plus(55).as("num2")
      )

      val expectedData = List(
        Row(45, 100),
        Row(79, 134),
        Row(0, 55),
        Row(124, 179)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, false),
        StructField("num2", IntegerType, false)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#rlike") {
    it("pattern match of a string pattern against pattern") {

      val sourceDf = Seq(
        ("A", "bbbbba11111xy"),
        ("B", "aaaab11111xy"),
        ("C", "aaaab22111xy"))
        .toDF("id", "text")

      val actualDf = sourceDf.filter($"text".rlike("b{5}.[1]{5}(x|y){2}"))

      val expectedDf = Seq(
        ("A", "bbbbba11111xy"))
        .toDF("id", "text")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#startsWith") {
    pending
  }

  describe("#substr") {

    it("An expression that returns a substring. substr(startPos: Int, len: Int): Column") {

      val sourceDf = Seq(
        ("mayflower"),
        ("nightshift"),
        (null),
        ("lamplight")
      ).toDF("word")

      val actualDf = sourceDf.select(
        sourceDf.col("word"),
        sourceDf.col("word").substr(4, 3).as("word_part")
      )

      val expectedData = List(
        Row("mayflower", "flo"),
        Row("nightshift", "hts"),
        Row(null, null),
        Row("lamplight", "pli")
      )

      val expectedSchema = List(
        StructField("word", StringType, true),
        StructField("word_part", StringType, true)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }



  describe("#replaceFirstIn") {
    pending

  }


  describe("#toString") {
    pending
  }

  describe("#unapply") {
    pending
  }

  describe("#when") {
    pending
  }

}
