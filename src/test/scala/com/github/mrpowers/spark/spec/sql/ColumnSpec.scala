package com.github.mrpowers.spark.spec.sql

import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, _}

import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class ColumnSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

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

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#and") {

    it("returns true if both columns are true") {

      val sourceDF = Seq(
        ("ted", true, false),
        ("mary", true, true),
        ("brian", false, true)
      ).toDF("name", "like_cheese", "is_tall")

      val actualDF = sourceDF.where(col("like_cheese").and(col("is_tall")))

      val expectedDF = Seq(
        ("mary", true, true)
      ).toDF("name", "like_cheese", "is_tall")

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      val sourceDF = Seq(
        ("Scala is scalable and cool"),
        ("I like peanuts"),
        (null)
      ).toDF("text")

      val actualDF = sourceDF.filter($"text".contains("Scala"))

      val expectedDF = Seq(
        ("Scala is scalable and cool")
      ).toDF("text")

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

    it("divides a column by a value") {

      val sourceDF = Seq(
        (9),
        (3),
        (27)
      ).toDF("num1")

      val actualDF = sourceDF.withColumn("num2", col("num1").divide(3))

      val expectedData = Seq(
        Row(9, 3.0),
        Row(3, 1.0),
        Row(27, 9.0)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, false),
        StructField("num2", DoubleType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#endsWith") {

    it("string ends with") {

      val sourceDF = Seq(
        ("A", "1970"),
        ("B", "1986"),
        ("C", "1990"),
        ("D", "2000"),
        ("E", "2012")
      ).toDF("id", "year")

      val actualDF = sourceDF.where(col("year").endsWith("0"))

      val expectedDF = Seq(
        ("A", "1970"),
        ("C", "1990"),
        ("D", "2000")
      ).toDF("id", "year")

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      val sourceDF = Seq(
        ("1980-09-10"),
        ("1990-04-18"),
        ("2000-04-18"),
        ("2010-04-18"),
        ("2016-01-10")
      ).toDF("some_date")
        .withColumn("some_date", $"some_date".cast("timestamp"))

      val actualDF = sourceDF.where(col("some_date").geq("1995-04-18"))

      val expectedDF = Seq(
        ("2000-04-18"),
        ("2010-04-18"),
        ("2016-01-10")
      ).toDF("some_date")
        .withColumn("some_date", $"some_date".cast("timestamp"))

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#getField") {
    pending
  }

  describe("#getItem") {

    it("An expression that gets an item at position ordinal out of an array, or gets a value by key key in a MapType") {

      val sourceDF = Seq(
        ("A", "1970-10-02"),
        ("B", "1986-12-30"),
        ("C", "1990-12-03"),
        ("D", "2000-04-22"),
        ("E", "2012-09-09")
      ).toDF("id", "birthday")

      val actualDF = sourceDF.withColumn("month", split(col("birthday"), "-").getItem(1))

      val expectedDF = Seq(
        ("A", "1970-10-02", "10"),
        ("B", "1986-12-30", "12"),
        ("C", "1990-12-03", "12"),
        ("D", "2000-04-22", "04"),
        ("E", "2012-09-09", "09")
      ).toDF("id", "birthday", "month")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#gt") {

    it("keeps rows greater than a certain number") {

      val sourceDF = Seq(
        (45),
        (79),
        (124),
        (196),
        (257)
      ).toDF("num1")

      val actualDF = sourceDF.where(col("num1").gt(124))

      val expectedDF = Seq(
        (196),
        (257)
      ).toDF("num1")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#hashCode") {
    pending
  }

  describe("#isin") {

    it("A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments") {

      val sourceDF = Seq(
        ("nate"),
        ("tim"),
        ("Tim"),
        ("miller"),
        ("jackson"),
        ("andrew")
      ).toDF("some_name")

      val actualDF = sourceDF.where(col("some_name").isin("tim", "andrew", "noname"))

      val expectedDF = Seq(
        ("tim"),
        ("andrew")
      ).toDF("some_name")

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      val sourceDF = Seq(
        (45),
        (79),
        (55),
        (124)
      ).toDF("num1")

      val actualDF = sourceDF.select(
        sourceDF.col("num1"),
        sourceDF.col("num1").minus(55).as("num2")
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

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#mod") {
    it("return modulo, remainder") {

      val sourceDF = Seq(
        ("A", "1990"),
        ("B", "1998"),
        ("C", "2011"),
        ("D", "2014"),
        ("E", "2017"),
        ("F", "2016"),
        ("G", "1993")
      ).toDF("id", "year")

      val actualDF = sourceDF.filter(col("year").mod(2) === 0)

      val expectedDF = Seq(
        ("A", "1990"),
        ("B", "1998"),
        ("D", "2014"),
        ("F", "2016")
      ).toDF("id", "year")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#multiply") {

    it("Multiplication of this expression and another expression.") {

      val sourceDF = Seq(
        (7),
        (14),
        (23),
        (41)
      ).toDF("num1")

      val actualDF = sourceDF.select(
        sourceDF.col("num1"),
        sourceDF.col("num1").multiply(5).as("num2")
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

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      val sourceDF = Seq(
        (45),
        (79),
        (0),
        (124)
      ).toDF("num1")

      val actualDF = sourceDF.select(
        sourceDF.col("num1"),
        sourceDF.col("num1").plus(55).as("num2")
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

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#rlike") {
    it("pattern match of a string pattern against pattern") {

      val sourceDF = Seq(
        ("A", "bbbbba11111xy"),
        ("B", "aaaab11111xy"),
        ("C", "aaaab22111xy")
      ).toDF("id", "text")

      val actualDF = sourceDF.filter($"text".rlike("b{5}.[1]{5}(x|y){2}"))

      val expectedDF = Seq(
        ("A", "bbbbba11111xy")
      ).toDF("id", "text")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#startsWith") {
    pending
  }

  describe("#substr") {

    it("An expression that returns a substring. substr(startPos: Int, len: Int): Column") {

      val sourceDF = Seq(
        ("mayflower"),
        ("nightshift"),
        (null),
        ("lamplight")
      ).toDF("word")

      val actualDF = sourceDF.select(
        sourceDF.col("word"),
        sourceDF.col("word").substr(4, 3).as("word_part")
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

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

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
