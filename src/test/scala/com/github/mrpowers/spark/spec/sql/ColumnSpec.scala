package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, _}

class ColumnSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

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
}
