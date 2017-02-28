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

}
