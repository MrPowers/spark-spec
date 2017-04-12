package com.github.mrpowers.spark.spec.sql

import org.scalatest.FunSpec
import com.github.mrpowers.spark.spec.SparkSessionTestWrapper

import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class RelationalGroupedDatasetSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  describe("#agg") {
    pending
  }

  describe("#apply") {
    pending
  }

  describe("#avg") {
    pending
  }

  describe("count") {

    it("groups by 2 columns and returns the count in the group") {

      val sourceDF = Seq(
        ("Reyes", "SS", "Mets"),
        ("Backman", "2B", "Mets"),
        ("Clemens", "P", "Red Sox"),
        ("Boggs", "3B", "Red Sox"),
        ("Gooden", "P", "Mets"),
        ("Darling", "P", "Mets"),
        ("Strawberry", "OF", "Mets")
      ).toDF("player", "position", "team")

      val expectedDF = Seq(
        ("3B", "Red Sox", 1L),
        ("2B", "Mets", 1L),
        ("P", "Mets", 2L),
        ("SS", "Mets", 1L),
        ("OF", "Mets", 1L),
        ("P", "Red Sox", 1L)
      ).toDF("position", "team", "count")

      val aRelationalGroupedDataSet = sourceDF.groupBy("position", "team")
      val actualDF = aRelationalGroupedDataSet.count
      assertSmallDataFrameEquality(expectedDF, actualDF)

    }

  }

  describe("#max") {
    pending
  }

  describe("#mean") {
    pending
  }

  describe("#min") {
    pending
  }

  describe("#pivot") {
    pending
  }

  describe("#sum") {
    pending
  }

}