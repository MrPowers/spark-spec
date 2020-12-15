package com.github.mrpowers.spark.spec.sql

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper
import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class ParquetPredicatePushdownFilteringSpec
  extends FunSpec
  with SparkSessionTestWrapper
  with DatasetComparer
{

  describe("CSV lake") {
    it("can't perform predicate pushdown filtering") {
      val path = os.pwd/"src"/"test"/"resources"/"people_csv"
      val schema = StructType(Seq(
        StructField("first_name", StringType, true),
        StructField("age", IntegerType, true)
      ))
      val csvDF = spark.read.option("header", true).schema(schema).csv(path.toString())
      csvDF.where(col("age") > 90).explain(true)
    }
  }

  describe("parquet lake") {
    it("performs predicate pushdown filtering on a Parquet lake") {
      val parquetPath = os.pwd/"src"/"test"/"resources"/"people_parquet"
      val parquetDF = spark.read.parquet(parquetPath.toString())
      parquetDF.where(col("age") > 90).explain(true)
    }
  }

}
