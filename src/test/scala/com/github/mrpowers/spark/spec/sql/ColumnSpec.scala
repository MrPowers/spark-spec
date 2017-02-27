package com.github.mrpowers.spark.spec.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._
import org.apache.spark.sql.functions._

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
      ).toDF("some_date")
      
      val actualDf = sourceDf.where(col("some_date").gt(124))
      
      val expectedDf = Seq(
        (196),
        (257)
      ).toDF("some_date")
      
      assertDataFrameEquals(actualDf, expectedDf)
      
    }
    
  } 

}
